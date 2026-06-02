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
    
    # STEP 4: Build the modular output DataFrame
    df_out = select(df_view, :source, :id)
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

# function gba_footprint_area_table(path="";
#   min_height::Float64=3.0,
#   region::AbstractString="ALL",
# )
#     files = 
#       isfile(path) ? path :
#       region == "ALL" && isdir(path) ? get_bbox_files(path) : 
#       get_relevant_files(path, region)
    
#     if isempty(files)
#         return DataFrame()
#     end
#     df = DataFrame(Arrow.Table(files))
#     df.region = PooledArray(df.region) 
#     df_view = subset(
#       df,
#       :height => h -> h .>= min_height, 
#       :region => r -> region == "ALL" ? trues(length(r)) : r .== region; 
#       view=true,
#       skipmissing=true,
#     )
    
#     if nrow(df_view) == 0
#         df_view.footprint_area_m2 = Float64[]
#         return df_view
#     end

#     # STEP 1: Fast EPSG Pass using bounding boxes and native math
#     epsg_codes = map(df_view.bbox) do box
#         center_lon = (box.xmin + box.xmax) / 2.0
#         center_lat = (box.ymin + box.ymax) / 2.0
        
#         zone_num = floor(Int, (center_lon + 180.0) / 6.0) + 1
#         is_north = center_lat >= 0.0
        
#         return center_lat >= 84.0 ? 32661 :
#                center_lat <= -80.0 ? 32761 :
#                is_north ? 32600 + zone_num :
#                32700 + zone_num
#     end
    
#     areas = zeros(Float64, nrow(df_view))
#     source_srs = ArchGDAL.importEPSG(4326; order=:trad)
#     unique_epsgs = unique(epsg_codes)
    
#     # PERFORMANCE FIX: Extract the geometry column OUTSIDE the loop
#     # This provides the strongly-typed array to our function barrier
#     geom_col = df_view.geometry
    
#     # STEP 2: Group by unique EPSG codes
#     for epsg in unique_epsgs
#         zone_indices = findall(==(epsg), epsg_codes)
#         target_srs = ArchGDAL.importEPSG(epsg)
        
#         # STEP 3: Initialize the PROJ transformation pipeline ONCE per zone
#         ArchGDAL.createcoordtrans(source_srs, target_srs) do trans
            
#             # STEP 4: Safely process the batch at full compiled speed
#             evaluate_zone_areas!(areas, geom_col, zone_indices, trans)
            
#         end
#     end
    
#     # Assign the evaluated areas back to the DataFrame
#     df_view.footprint_area_m2 = areas
    
#   @info "Used . . . $files."
#   return df_view
# end

# function gba_footprint_area_table(path="";
#   min_height::Float64=3.0,
#   region::AbstractString="ALL",
# )
#     files = 
#       isfile(path) ? path :
#       region == "ALL" && isdir(path) ? get_bbox_files(path) : 
#       get_relevant_files(path, region)
    
#     if isempty(files)
#         return DataFrame()
#     end
#     df = DataFrame(Arrow.Table(files))
#     df.region = PooledArray(df.region) 
#     df_view = subset(
#       df,
#       :height => h -> h .>= min_height, 
#       :region => r -> region == "ALL" ? trues(length(r)) : r .== region; 
#       view=true,
#       skipmissing=true,
#     )
    
#     # Safety check in case the subset filters out all rows
#     if nrow(df_view) == 0
#         df_view.footprint_area_m2 = Float64[]
#         return df_view
#     end

#     # STEP 1: Fast EPSG Pass using bounding boxes and native math
#     # Bypasses the Geodesy.utm_zone() bottleneck completely
#     epsg_codes = map(df_view.bbox) do box
#         center_lon = (box.xmin + box.xmax) / 2.0
#         center_lat = (box.ymin + box.ymax) / 2.0
        
#         # Calculate standard 6-degree UTM zone mathematically 
#         zone_num = floor(Int, (center_lon + 180.0) / 6.0) + 1
#         is_north = center_lat >= 0.0
        
#         return center_lat >= 84.0 ? 32661 :
#                center_lat <= -80.0 ? 32761 :
#                is_north ? 32600 + zone_num :
#                32700 + zone_num
#     end
    
#     # Pre-allocate areas array
#     areas = zeros(Float64, nrow(df_view))
    
#     # Define source SRS once (using traditional X/Y lon/lat ordering) [1]
#     source_srs = ArchGDAL.importEPSG(4326; order=:trad)
    
#     # STEP 2: Group by unique EPSG codes
#     unique_epsgs = unique(epsg_codes)
    
#     for epsg in unique_epsgs
#         # Find all row indices belonging to this specific UTM zone
#         zone_indices = findall(==(epsg), epsg_codes)
        
#         target_srs = ArchGDAL.importEPSG(epsg)
        
#         # STEP 3: Initialize the PROJ transformation pipeline ONCE per zone [2, 3]
#         ArchGDAL.createcoordtrans(source_srs, target_srs) do trans
            
#             # STEP 4: Parse, transform, and evaluate the geometries for this zone
#             for i in zone_indices
#                 geom = ArchGDAL.fromWKB(df_view.geometry[i])
                
#                 ArchGDAL.flattento2d!(geom)
#                 ArchGDAL.closerings!(geom)
                
#                 ArchGDAL.transform!(geom, trans)
#                 areas[i] = ArchGDAL.geomarea(geom)
#             end
#         end
#     end
    
#     # Assign the evaluated areas back to the DataFrame
#     df_view.footprint_area_m2 = areas
    
#   @info "Used . . . $files."
#   return df_view
# end

# function gba_footprint_area_table(path="";
#   min_height::Float64=3.0,
#   region::AbstractString="ALL",
# )
#     files = 
#       isfile(path) ? path :
#       region == "ALL" && isdir(path) ? get_bbox_files(path) : 
#       get_relevant_files(path, region)
    
#     if isempty(files)
#         return DataFrame()
#     end
#     df = DataFrame(Arrow.Table(files))
#     df.region = PooledArray(df.region) 
#     df_view = subset(
#       df,
#       :height => h -> h .>= min_height, 
#       :region => r -> region == "ALL" ? trues(length(r)) : r .== region; 
#       view=true,
#       skipmissing=true,
#     )
    
#     # Safety check in case the subset filters out all rows
#     if nrow(df_view) == 0
#         df_view.footprint_area_m2 = Float64[]
#         return df_view
#     end
    
#     # OPTIMIZATION: Calculate the EPSG code exactly ONCE for the entire tile
#     # using the bounding box of the very first building to represent the whole area.
#     first_box = df_view.bbox[2]
#     center_lon = (first_box.xmin + first_box.xmax) / 2.0
#     center_lat = (first_box.ymin + first_box.ymax) / 2.0
    
#     zone_num, is_north = Geodesy.utm_zone(center_lat, center_lon) # [1]
    
#     epsg_code = 
#         center_lat >= 84.0 ? 32661 :
#         center_lat <= -80.0 ? 32761 :
#         is_north ? 32600 + zone_num :
#         32700 + zone_num

#     # Define Spatial References ONCE
#     source_srs = ArchGDAL.importEPSG(4326; order=:trad)
#     target_srs = ArchGDAL.importEPSG(epsg_code)
    
#     # Wrap the entire map operation inside the coordinate transform do-block.
#     # This evaluates the entire tile using a single C++ PROJ pipeline.
#     df_view.footprint_area_m2 = ArchGDAL.createcoordtrans(source_srs, target_srs) do trans
        
#         map(df_view.geometry) do wkb_bytes
#             geom = ArchGDAL.fromWKB(wkb_bytes)
            
#             ArchGDAL.flattento2d!(geom)
#             ArchGDAL.closerings!(geom)
            
#             ArchGDAL.transform!(geom, trans)
#             return ArchGDAL.geomarea(geom)
#         end
        
#     end
    
#   @info "Used . . . $files."
#   return df_view
# end

# function gba_footprint_area_table(path="";
#   min_height::Float64=3.0,
#   region::AbstractString="ALL",
# )
#     files = 
#       isfile(path) ? path :
#       region == "ALL" && isdir(path) ? get_bbox_files(path) : # Just grab everything
#       get_relevant_files(path, region)
    
#     if isempty(files)
#         return DataFrame()
#     end
#     df = DataFrame(Arrow.Table(files))
#     df.region = PooledArray(df.region) # Convert to PooledArray for memory efficiency
#     df_view = subset(
#       df,
#       :height => h -> h .>= min_height, 
#       :region => r -> region == "ALL" ? trues(length(r)) : r .== region; 
#       view=true,
#       skipmissing=true,
#     )
#     source_srs = ArchGDAL.importEPSG(4326; order=:trad)
#     df_view.footprint_area_m2 = map(df_view.geometry) do wkb_bytes
#       geom = unsafe_wrap(
#         Vector{UInt8},
#         pointer(wkb_bytes),
#         length(wkb_bytes);
#         own=false,
#       )
#       geom = ArchGDAL.fromWKB(wkb_bytes)
      
#       # 2. Flatten to strictly 2D and close rings
#       ArchGDAL.flattento2d!(geom)
#       ArchGDAL.closerings!(geom)
      
#       # 3. Compute centroid to find the polygon's geographic center
#       centroid = ArchGDAL.centroid(geom)
#       lon = ArchGDAL.getx(centroid, 0)
#       lat = ArchGDAL.gety(centroid, 0)
      
#       # 4. Programmatically determine the UTM zone and EPSG code
#       zone_num, is_north = Geodesy.utm_zone(lat, lon)
      
#       epsg_code =
#         lat >= 84.0 ? 32661 :
#         lat <= -80.0 ? 32761 :
#         is_north ? 32600 + zone_num :
#         32700 + zone_num

#       # 5. Define Spatial References
#       # Use order=:trad to override strict EPSG lat/lon ordering and prevent 
#       # PROJ 6 / GDAL 3 coordinate transpositions for legacy GIS data [6-8, 12]
#       target_srs = ArchGDAL.importEPSG(epsg_code)

#       # 6. Execute the coordinate transformation safely
#       ArchGDAL.createcoordtrans(source_srs, target_srs) do trans
#           ArchGDAL.transform!(geom, trans)
#       end
#     end
#   @info "Used . . . $files."
#   df_view
# end


# function calculate_wkb_footprint_area(wkb_bytes::Vector{UInt8})
#     # 1. Parse the WKG Polygon from WKB
#     geom = ArchGDAL.fromWKB(wkb_bytes)
    
#     # 2. Flatten to strictly 2D and close rings
#     ArchGDAL.flattento2d!(geom)
#     ArchGDAL.closerings!(geom)
    
#     # 3. Compute centroid to find the polygon's geographic center
#     centroid = ArchGDAL.centroid(geom)
#     lon = ArchGDAL.getx(centroid, 0)
#     lat = ArchGDAL.gety(centroid, 0)
    
#     # 4. Programmatically determine the UTM zone and EPSG code
#     zone = Geodesy.utm_zone(lat, lon) # Handles complex edge cases like Svalbard [13]
    
#     if lat >= 84.0
#         epsg_code = 32661 # UPS North (WGS 84) [2]
#     elseif lat <= -80.0
#         epsg_code = 32761 # UPS South (WGS 84) [4]
#     else
#         # Dynamic UTM mapping based on Hemisphere [1, 2, 4]
#         epsg_code = lat >= 0.0 ? 32600 + zone : 32700 + zone
#     end
    
#     # 5. Define Spatial References
#     # Use order=:trad to override strict EPSG lat/lon ordering and prevent 
#     # PROJ 6 / GDAL 3 coordinate transpositions for legacy GIS data [6-8, 12]
#     source_srs = ArchGDAL.importEPSG(4326; order=:trad)
#     target_srs = ArchGDAL.importEPSG(epsg_code)
    
#     # 6. Execute the coordinate transformation safely
#     ArchGDAL.createcoordtrans(source_srs, target_srs) do trans
#         ArchGDAL.transform!(geom, trans)
#     end
    
#     # 7. Evaluate and return the metric planar surface area
#     return ArchGDAL.geomarea(geom)
# end

# --- Example Usage ---
# Create a valid test geometry using Well-Known Text (WKT) 
# (e.g., a 1-degree square roughly off the coast of West Africa)
# wkt_string = "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))"
# geom_test = ArchGDAL.fromWKT(wkt_string)

# # Convert the geometry into a valid Well-Known Binary (WKB) byte array
# wkb_data = ArchGDAL.toWKB(geom_test)

# footprint_sqm = calculate_wkb_footprint_area(wkb_data)
# println("Calculated Planar Footprint Area: ", footprint_sqm, " sq. meters")
