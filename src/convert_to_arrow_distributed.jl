using Distributed

# 1. THE FLEET: Spin up 16 independent Julia worker processes
# This replaces `julia -t 16`. You just run `julia script.jl` and it spawns them internally.
addprocs(16) 

# 2. THE BLUEPRINT: Tell EVERY worker what tools they need and how to do the job.
# The @everywhere macro ensures this code is loaded into the isolated memory of all 16 workers.
@everywhere begin
    using Glob
    using DuckDB
    using Arrow

    # Define the isolated task
    function convert_file(file::String)
        out_file = replace(file, ".parquet" => ".arrow")
        tmp_file = out_file * ".tmp"

        if isfile(out_file)
            return :skipped
        end

        # Because this is a separate process, this connection is 100% isolated.
        con = DBInterface.connect(DuckDB.DB, ":memory:")

        try
            # No race conditions! Each worker loads its own copy of the C++ extension into its own RAM.
            DBInterface.execute(con, "LOAD spatial;") 
            DBInterface.execute(con, "PRAGMA memory_limit='10GB';")

            query = """
                SELECT * EXCLUDE (geometry), ST_AsWKB(geometry) AS geometry 
                FROM read_parquet('$file')
            """

            Arrow.write(tmp_file, DBInterface.execute(con, query))
            mv(tmp_file, out_file; force=true)
            
            return :success
        catch e
            isfile(tmp_file) && rm(tmp_file)
            # FAULT TOLERANCE: Instead of crashing the whole script, just return the error to the Master!
            return (file => e) 
        finally
            close(con)
        end
    end
end

# 3. THE MASTER: Back on the main process, find the work and hand it out.
using Glob
target_dir = "." 
parquet_files = glob("*.parquet", target_dir)

@info "Distributing $(length(parquet_files)) files across $(nworkers()) isolated workers..."

# 4. THE DISPATCHER: `pmap` (Parallel Map) automatically feeds files to workers as they become available.
results = pmap(convert_file, parquet_files)

# 5. THE AUDIT: Check if any workers returned a "Poison Pill" error instead of crashing.
failures = filter(r -> r isa Pair, results)

if isempty(failures)
    @info "Batch processing complete! Zero Segfaults."
else
    @error "Job finished, but $(length(failures)) files contained corrupted geometries:"
    for (file, err) in failures
        println(" - $file failed with: $err")
    end
end
