using Glob
using DuckDB
using Arrow
using Base.Threads

target_dir = "." 
parquet_files = glob("*.parquet", target_dir)

@info "Found $(length(parquet_files)) files to process."
@info "Spinning up on $(Threads.nthreads()) cores..."

# --- THE FIX: WARM UP ---
# Create one dummy connection to install/load everything globally first
let
    @info "Performing global extension warm-up..."
    temp_con = DBInterface.connect(DuckDB.DB, ":memory:")
    DBInterface.execute(temp_con, "INSTALL spatial;")
    DBInterface.execute(temp_con, "LOAD spatial;")
    close(temp_con)
end
# ------------------------

Threads.@threads for file in parquet_files
    out_file = replace(file, ".parquet" => ".arrow")
    tmp_file = out_file * ".tmp"
    
    if isfile(out_file)
        continue
    end

    @info "Starting: $file"
    
    # We still use local connections for thread isolation
    local con = DBInterface.connect(DuckDB.DB, ":memory:")
    
    try
        # Since it's loaded globally, this is now a "no-op" or a safe local link
        DBInterface.execute(con, "LOAD spatial;") 
        DBInterface.execute(con, "PRAGMA memory_limit='8GB';")
        
        query = """
            SELECT * EXCLUDE (geometry), ST_AsWKB(geometry) AS geometry 
            FROM read_parquet('$file')
        """
        
        Arrow.write(tmp_file, DBInterface.execute(con, query))
        mv(tmp_file, out_file; force=true)
        @info "Converted: $file"
    catch e
        @warn "Error processing $file" exception=e
        isfile(tmp_file) && rm(tmp_file)
    finally
        close(con)
    end
end

@info "Batch processing complete!"
