"""
LLM-generated code to illustrate a way to do the population distribution with no/low/lazy
storage/CPU usage.
""""
function distribute_population(con, tiles, known_regional_population; base_dir="Arrow_Partitioned/")
    
    # 1. Build the DuckDB file array syntax
    paths = ["'$(base_dir)continent/$(String(t)).arrow'" for t in tiles]
    file_list_str = "[" * join(paths, ", ") * "]"
    
    # 2. The Proportional Allocation Recipe
    query = """
        WITH building_volumes AS (
            SELECT 
                id,
                -- We only need the coordinates for assigning people, so we can extract the centroid
                ST_Centroid(ST_GeomFromWKB(geometry)) AS building_center,
                -- Calculate volume, imputing 3.5m for missing OSM heights
                ST_Area_Spheroid(ST_GeomFromWKB(geometry)) * (CASE WHEN height <= 0 THEN 3.5 ELSE height END) AS volume_m3
            FROM $file_list_str
        )
        SELECT 
            id,
            building_center,
            volume_m3,
            -- Calculate the building's relative % of the total volume
            volume_m3 / SUM(volume_m3) OVER () AS relative_weight,
            
            -- Assign the specific number of people to this specific building
            (volume_m3 / SUM(volume_m3) OVER ()) * $known_regional_population AS assigned_people
            
        FROM building_volumes
    """
    
    return DataFrame(DBInterface.execute(con, query))
end
