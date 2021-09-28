import argparse
from timeit import default_timer as timer
from arcpy.sa import *
from pathlib import *
import os
import arcpy
import sys
import pandas as pd
import shutil

def create_and_calc_uid(out_segments, OBJID, UID):
    """ create uid field as text and copy over objectid """
    st = time.time()
    
    # create field
    arcpy.AddField_management(str(out_segments), UID, "LONG")

    # calculate min and max object id values so they can be run chunks
    min_value = arcpy.da.SearchCursor(
        str(out_segments), [OBJID], "{} IS NOT NULL".format(OBJID), sql_clause = (None, "ORDER BY {} ASC".format(OBJID))
        ).next()[0]
    max_value = arcpy.da.SearchCursor(
        str(out_segments), [OBJID], "{} IS NOT NULL".format(OBJID), sql_clause = (None, "ORDER BY {} DESC".format(OBJID))
        ).next()[0]
    
    increment = 1000000 # read items chunks of one million

    # loop through million increments and construct query on objectids
    for x in range(min_value, max_value, increment):
        mn, mx = x, x-min_value+increment
        batch_query = f'"{OBJID}" >= {mn} AND "{OBJID}" <= {mx}'
        # run batch_query and populate uid field
        with arcpy.da.UpdateCursor(str(out_segments), [OBJID, UID], batch_query) as cursor:
            for row in cursor:
                row[1] = int(row[0]) # write out as strings
                cursor.updateRow(row)

    end = round((time.time() - st)/60.0, 2)
    print(f"Complete populating UIDs: {end} mins")
    
    return min_value, max_value, increment

def dissolve_layers(out_segments, UID):
    """ 
        1. Make feature layers for each class and Dissolve field with 1 OR 2
        2. Get their IDs so they can be deleted later
        3. Dissolve each feature class by Class_name and Dissolve
        4. Append the resulting feature layers to the list 
    """
    st = time.time()
    classes = {
        "lv": "Low Vegetation", 
        "ss": "Scrub\\Shrub", 
        "br": "Barren",
        "tc": "Tree Canopy",
    }

    object_ids_to_delete = []
    layers_to_merge = []
    classes_to_dissolve = ["lv", "ss", "br",]
    
    for c in classes.items():
        # create feature layer
        layer = f"{c[0]}_tmp"
        query = f"Class_name = '{c[1]}'"
        if c[0] not in classes_to_dissolve:
            query += " And (Dissolve = 1 Or Dissolve = 2)"
        arcpy.management.MakeFeatureLayer(str(out_segments), layer, query)
        
        # print number of features that intersect the tiles
        fc_count = arcpy.GetCount_management(layer)
        print(f'{layer} has {fc_count[0]} records')
        
        # get ids to delete
        del_ids = [r[0] for r in arcpy.da.SearchCursor(layer, [UID])]
        object_ids_to_delete += del_ids

        # dissolve by class_name and dissolve field
        tmp_dissolve = rf"memory\tmp_{c[0]}_dissolve"
        arcpy.management.Dissolve(
            layer, tmp_dissolve, ["Class_name", "Dissolve"], None, "SINGLE_PART", "DISSOLVE_LINES"
            )
        
        # append dissolve layers to a list
        layers_to_merge.append(tmp_dissolve)

        print(f'Dissolving: {c[1]}')

    end = round((time.time() - st)/60.0, 2)
    print(f"Complete Dissolving all classes: {end} mins")
    
    return object_ids_to_delete, layers_to_merge

def delete_dissolved_object_ids(out_segments, UID, object_ids_to_delete):
    st = time.time()

    # convert list into sql list syntax i.e. [1,2,3] > '1,2,3'
    uid_list = ','.join(map(repr, object_ids_to_delete))

    # query to get all values that match uid_list
    query = f"uid IN ({uid_list})"

    # run update cursor and delete the rows
    with arcpy.da.UpdateCursor(str(out_segments), [UID], query) as cursor:
        for x, row in enumerate(cursor):
            cursor.deleteRow()

    end = round((time.time() - st)/60.0, 2)
    print(f"Deleting dissolved features: {end} mins")

def append_segments(layers_to_merge, out_segments):
    """
    loop through each dissolved layer and append back to out_segments
    """
    st = time.time()

    for layer in layers_to_merge:
        arcpy.management.Append(layer, str(out_segments), "NO_TEST")

    end = round((time.time() - st)/60.0, 2)
    print(f"All features appended: {end} mins")

def dissolve_segs(segments, out_segments):
    """
    1. Convert tile index from Polygon to Polyline
    2. Buffer by 1m
    3. Create unique id field
    3. Make three feature layers (implement multiprocessing)
        1. low veg
        2. scrub shrub
        3. barren
        4. tree canopy
    4. Perform spatial selection on each layer with select
    5. Keep a list of OBJECTIDs either in memory or write it out
    4. Perform dissolve with multipart as unchecked 
    """
    start_time = time.time()

    # environments
    arcpy.env.overwriteOutput = True

    # check if input file exists:
    features = [segments, out_segments]
    for feature in features:
        suffix = feature.parent.suffix
        if suffix != '.gdb':
            print("Non feature class file passed. Only accepted input is feature class inside geodatabase. Exiting...")
            sys.exit(1)
    
    if arcpy.Exists(segments):

        # create copy of the original file
        arcpy.management.Copy(str(segments), str(out_segments))

        # fields hardcoded for now
        OBJID = "OBJECTID"
        UID = "uid"

        # create and calc uid field
        min_value, max_value, increment = create_and_calc_uid(out_segments, OBJID, UID)

        # dissolve classes and identify object ids to delete
        object_ids_to_delete, layers_to_merge = dissolve_layers(out_segments, UID)

        # create index to speed up deleting edits
        # this maybe yield negligible performance as i run sql queries on a smaller subset - idk!??!?!? 
        arcpy.management.AddIndex(str(out_segments), [UID], "idx", "UNIQUE", "ASCENDING")

        # run Update Cursor to run and delete objectids that were dissolved
        delete_dissolved_object_ids(out_segments, UID, object_ids_to_delete)

        append_segments(layers_to_merge, out_segments)

        end_time = round((time.time() - start_time)/60.0, 2)
        print('Total processing time: ', end_time)

def calculate_max(file, field, query_is_not):
    query = f"{field} {query_is_not}"
    return arcpy.da.SearchCursor(file, [field], query, sql_clause=(None, "ORDER BY {} DESC".format(field))).next()[0]

def set_temp_folders(temp_folder):
    temp_path = temp_folder / 'temp_alignment'

    # if the temp folder doesn't 
    if not temp_path.is_file():
        os.mkdir(temp_path)

    os.environ['TMP'] = str(temp_path)
    os.environ['ESRI_OS_DIR_DONOTUSE'] = str(temp_path)
    os.environ['TEMP'] = str(temp_path)

    delete_list = list(temp_path.rglob('*')) # list of all temp files

    for i in temp_del_list:
        if os.path.isfile(i):
            os.remove(i)
        elif os.path.isdir(i):
            shutil.rmtree(i)

    del delete_list
    return 

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Dissolving Segments Script'
        )
    parser.add_argument('-batch', type=str, help='batch file')

    args = parser.parse_args()
    batch = pd.read_csv(args.batch)
    batch_records = batch.to_dict('records')

    for fname in batch_records:
        segments = Path(fname['segs'])
        out_segments = Path(fname['o_segs'])
        lc_raw = Path(fname['lc_raw'])
        lc_albers = Path(fname['lc_albers'])
        segs_aligned = Path(fname['aligned_segs'])
        segs_aligned = Path(fname['aligned_segs'])
        
        # set temp folders and clean up any leftovers
        set_temp_folders(lc_albers.parent)
        
        arcpy.env.workspace = str(out_segments.parent)
        arcpy.env.overwriteOutput = True

        # start
        start = timer()
        if arcpy.CheckExtension("Spatial") == "Available":
            arcpy.CheckOutExtension("Spatial")
        else:
            arcpy.AddError("Unable to get spatial analyst extension")
            arcpy.AddMessage(arcpy.GetMessages(0))
            sys.exit(0)
        
        # dissolve segs
        dissolve_segs(segments, out_segments)

        # Step - 1: rasterize segments (in native projection)
        st = timer()
        rasterized_segs_native = str(out_segments.parent / "rasterized_segs_native")
        arcpy.conversion.PolygonToRaster(str(out_segments), "OBJECTID", rasterized_segs_native, "CELL_CENTER", "NONE", str(lc_raw))
        print("Step 1: Rasterizing segments complete", round((timer()-st)/60.0, 2))

        # Step - 2: project rasterized segments to albers
        st = timer()
        arcpy.env.snapRaster = str(lc_albers)
        arcpy.env.cellSize = 1
        rasterized_segs_albers = str(out_segments.parent / "rasterized_segs_albers")
        albers_spatial_reference = arcpy.SpatialReference(102039)

        arcpy.management.ProjectRaster(
            rasterized_segs_native, rasterized_segs_albers, albers_spatial_reference, "NEAREST", "1", vertical="NO_VERTICAL"
            )

        arcpy.management.Delete(rasterized_segs_native)
        print("Step 2: Project rasterized segments to albers complete", round((timer()-st)/60.0, 2))

        # Step - 3: Vectorize the projected segments and join the attributes back based on FID/ObjectID
        st = timer()
        temp_segs = str(out_segments.parent / "temp_segs")
        arcpy.conversion.RasterToPolygon(rasterized_segs_albers, temp_segs, "NO_SIMPLIFY", "Value", "SINGLE_OUTER_PART", None)
        arcpy.management.Delete(rasterized_segs_albers)
        print("Step 3: Vectorizing the raster segments complete", round((timer()-st)/60.0, 2))

        # Step - 4: Join class_name and dissolve back to the segments
        st = timer()
        # add fields
        add_fields = [['Class_name', 'TEXT'], ['Dissolve', 'Double']]
        arcpy.management.AddFields(temp_segs, add_fields)

        # fields
        fields = ["Class_name", "Dissolve"]

        # get Class_name values
        seg_data_to_join = {r[0]: (r[1], r[2]) for r in arcpy.da.SearchCursor(str(out_segments), ["OBJECTID"] + fields)}

        # iterate over 100k rows and update class_name and dissolve fields
        step = 100000
        max_value = calculate_max(temp_segs, "OBJECTID", "IS NOT NULL")
        for x in range(0, max_value, step):
            min = x
            max = x - 1 + step
            batch_query = f'"OBJECTID" >= {min} AND "OBJECTID" <= {max}'
            with arcpy.da.UpdateCursor(temp_segs, ['gridcode'] + fields, batch_query) as cursor:
                for row in cursor:
                    # class name
                    row[1] = seg_data_to_join[row[0]][0]
                    # dissolve
                    row[2] = seg_data_to_join[row[0]][1]
                    cursor.updateRow(row)
        print("Step 4: Fields joined back", round((timer()-st)/60.0, 2))

        # write out aligned segments from memory
        st = timer()
        arcpy.CopyFeatures_management(temp_segs, str(segs_aligned))
        arcpy.management.Delete(temp_segs)
        print("Step 5: Aligned segments saved", round((timer()-st)/60.0, 2))

        end = round((timer()-start)/60.0, 2)
        print(f"Total processing time: {end} mins")
