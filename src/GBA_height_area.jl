using Arrow
using ArchGDAL
using Geodesy
using DataFrames
using PooledArrays
using Dagger

function get_bbox_files(directory::String)
    pattern = r"^[ew]\d{3}_[ns]\d{2}_[ew]\d{3}_[ns]\d{2}\.arrow$"
    valid_files = filter(f -> occursin(pattern, f), readdir(directory))
    joinpath.(directory, valid_files)
end

function get_relevant_files(path::AbstractString, region::AbstractString)
    files = String[]
    
    # If a single file was passed, just return it
    if !isdir(path)
        push!(files, path)
        return files
    end

    all_bbox_files = get_bbox_files(path)
    file_lock = ReentrantLock()
    
    Threads.@threads for file in all_bbox_files
        @info "Checking file $file for region $region..."
        tbl = Arrow.Table(file)
        if any(isequal(region), tbl.region)
            @info "File $file contains data for region $region."
            lock(file_lock) do
                push!(files, file)
            end
        end
    end
    files
end

"""
Pool the 'region' column in all Arrow files in the specified directory
to increase speed of region selection.
Set `outpath` to a different directory if you don't want to overwrite the original files.
"""
function pool_arrow_region_column(inpath, outpath=inpath)
    @assert isdir(inpath) "$inpath must be a directory."
    @assert isdir(outpath) "$outpath must be a directory."
    files = get_bbox_files(inpath)
    Threads.@threads for file in files
        @info "Pooling region column in file $file..."
        tbl = Arrow.Table(file)
        if hasproperty(tbl, :region)
            out_file = joinpath(outpath, basename(file))
            df = DataFrame(tbl)
            df.region = PooledArray(df.region; signed=true) # Convert to PooledArray for memory efficiency
            Arrow.write(out_file, df; file=true)
            @info "Wrote to region pooled file:\n$file."
        else
            @warn "File $file does not contain a 'region' column. Skipping."
        end
    end
    readdir(outpath)
end

# HELPER FUNCTION: Acts as a "Function Barrier" to guarantee compiled C-speed
function evaluate_zone_areas!(areas, geom_col, zone_indices, trans)
    for i in zone_indices
        geom = ArchGDAL.fromWKB(geom_col[i])
        ArchGDAL.flattento2d!(geom)
        ArchGDAL.closerings!(geom)
        ArchGDAL.transform!(geom, trans)
        areas[i] = ArchGDAL.geomarea(geom)
    end
end

function calc_footprint_area_table(file::String; min_height::Float64=3.0, region::AbstractString="ALL")
    # Generate the output filename by appending "_area" to the stem
    base, ext = splitext(file)
    out_file = base * "_area" * ext

    df = DataFrame(Arrow.Table(file))
    
    # Fast exit if empty
    if nrow(df) == 0
        return out_file
    end
    
    df.region = PooledArray(df.region) 
    df_view = subset(
      df,
      :height => h -> h .>= min_height, 
      :region => r -> region == "ALL" ? trues(length(r)) : r .== region; 
      view=true,
      skipmissing=true,
    )
    
    # Handle case where the subset leaves no rows
    if nrow(df_view) == 0
        df_out = DataFrame(
            source = PooledArray(eltype(df.source)[]), 
            id = PooledArray(eltype(df.id)[]), 
            footprint_area = Float64[] 
        )
        Arrow.write(out_file, df_out; file=true)
        return out_file
    end

    # STEP 1: Fast EPSG Pass
    epsg_codes = map(df_view.bbox) do box
        center_lon = (box.xmin + box.xmax) / 2.0
        center_lat = (box.ymin + box.ymax) / 2.0
        
        zone_num = floor(Int, (center_lon + 180.0) / 6.0) + 1
        is_north = center_lat >= 0.0
        
        return center_lat >= 84.0 ? 32661 :
               center_lat <= -80.0 ? 32761 :
               is_north ? 32600 + zone_num :
               32700 + zone_num
    end
    
    areas = zeros(Float64, nrow(df_view))
    source_srs = ArchGDAL.importEPSG(4326; order=:trad)
    unique_epsgs = unique(epsg_codes)
    
    geom_col = df_view.geometry
    
    # STEP 2 & 3: Group and Transform
    for epsg in unique_epsgs
        zone_indices = findall(==(epsg), epsg_codes)
        target_srs = ArchGDAL.importEPSG(epsg)
        
        ArchGDAL.createcoordtrans(source_srs, target_srs) do trans
            evaluate_zone_areas!(areas, geom_col, zone_indices, trans)
        end
    end
    
    # STEP 4: Build the modular output DataFrame with exact pool matching
    # Create PooledArrays using similar() to inherit df's pools
    df_out = DataFrame(
        source = similar(df.source, nrow(df_view)),
        id = similar(df.id, nrow(df_view))
    )
    
    # Populate with broadcast assignment to maintain pool identity
    df_out.source .= df_view.source
    df_out.id .= df_view.id
    df_out.footprint_area = areas
    
    # Write to Arrow with the file=true option
    Arrow.write(out_file, df_out; file=true)
    
    @info "Processed and saved: $out_file"
    
    # Return just the path, keeping Dagger lightweight
    return out_file
end

function process_all_gba_files(path=""; min_height::Float64=3.0, region::AbstractString="ALL")
    # Gather all files
    files = 
      isfile(path) ? [path] 2:
      region == "ALL" && isdir(path) ? get_bbox_files(path) : 
      get_relevant_files(path, region)
    
    if isempty(files)
        return nothing
    end
    
    @info "Dispatching $(length(files)) files to the Dagger scheduler."
    
    # Fire-and-forget parallel loop
    # The @sync macro acts as a barrier, forcing the parent thread to wait 
    # until all spawned tasks inside the block have completely finished.
    @sync for file in files
        Dagger.@spawn calc_footprint_area_table(file; min_height=min_height, region=region)
    end
    
    @info "Finished processing . . . $files"
end

function pool_source_id_single_file(file::String, out_dir::String)
    df = DataFrame(Arrow.Table(file))
    out_file = joinpath(out_dir, basename(file))
    
    # Fast exit if the file is completely empty
    if nrow(df) == 0
        # Just copy the empty file over to maintain partition structure
        cp(file, out_file; force=true)
        return out_file
    end
    
    # Re-encode source and id as PooledArrays 
    df.source = PooledArray(df.source)
    df.id = PooledArray(df.id)
    
    # Write back to Arrow using the random-access IPC format
    Arrow.write(out_file, df; file=true)
    
    @info "Pooled and saved: $out_file"
    return out_file
end

function pool_source_id_batch(input_dir::String; out_dir::String = input_dir * "_pool_source_id")
    # Ensure the output directory exists before workers try to write to it
    mkpath(out_dir)
    
    # Regex to strictly match the tile naming convention: e.g., w115_n25_w110_n20.arrow
    # This prevents picking up "_area.arrow" or other intermediate files.
    tile_pattern = r"^[ew]\d+_[ns]\d+_[ew]\d+_[ns]\d+\.arrow$"i
    
    # Gather only the files that match the exact coordinate pattern
    files = filter(readdir(input_dir, join=true)) do f
        occursin(tile_pattern, basename(f))
    end
    
    if isempty(files)
        @warn "No valid tile files found matching the pattern in $input_dir"
        return nothing
    end
    
    @info "Dispatching $(length(files)) files to the Dagger scheduler."
    
    # Fire-and-forget parallel loop using Dagger
    # The @sync block acts as a barrier, waiting until all tasks finish
    @sync for file in files
        Dagger.@spawn pool_source_id_single_file(file, out_dir)
    end
    
    @info "Finished pooling all files. Saved to: $out_dir"
    return nothing
end
