# Data-Wrangling-Toolbox-For-ArcGIS
### The Data Wrangling Toolbox is a growing collection of ArcGIS Python tools used for common GIS ETL (Extract, Transform, and Load), pre-processing, and profiling tasks within ArcGIS.  

##### Python:
```python
# Import Toolbox Into ArcGIS Python Env
DataWranglingTB = r'C:\YourFilePath\Data Wrangling_v0.0.1.pyt'
dw = arcpy.ImportToolbox(DataWranglingTB)
```

# Tools:

## 1. Unzip Directory:
Iterates a root folder/directory to recursively un-zip all files in place.

##### Python:
```python
# Unzip a directory
dw.unzipDir( 
              wksp,           # input root folder
           )
```


## 2. Get Metadata:
Iterates a workspace, (GDB, MDB, FeatureDataset, and/or folder) to look for feature classes to document/dump feature level metadata and field level data schemas in a multi-sheet excel file.

##### Python:
```python
# get workspace featureclass metadata
out_file = dw.getMetadata(
                           wksp,          # input root folder
                           rpt_dir,       # output file directory
                           rpt_name,      # output metadata file name
                           opn_op,        # boolean option to open once complete
                           src_type,      # list of ESRI file/workspace types to include ['.shp', '.gdb', '.mdb']
                           shape_types    # list of geometry types to query ('Polygon', 'Polyline','Point', 'Multipoint', and 'MultiPatch')
                         )
```

## 3. Create Workspace Feature Cross Reference:
Iterates a workspace, (GDB, MDB, FeatureDataset, and/or folder) to create a field level cross reference file. Filters can be applied to the data source type (.shp, .gdb, .mdb).  The tool will pull by shape type (i.e. Polygon, Point, Polyline...).  A target schema can can be set and the tool will attempt to use enhanced sequence matching algorithms to match input fields the target schema.  The output is an excel file with data validation on the allowed output columns/field names.

##### Python:
```python
dw.createWorkspaceFeatCrossRef(
                                wksp,           # input root folder
                                rpt_dir,        # output file directory
                                rpt_name,       # output cross reference file name
                                opn_op,         # boolean option to open once complete
                                src_types,      # list of ESRI file/workspace types to include ['.shp', '.gdb', '.mdb']
                                shape_types,    # geometry type to query ('Polygon', 'Polyline','Point', 'Multipoint', or 'MultiPatch')
                                tgt_schema      # Optional output schema to map fields to (to_feild)
                              )
```

## 4. Merge Features On Cross Reference:
Iterates file paths in a cross reference file created by the 'Create Workspace Feature Cross Reference' tool and uses the field mapping/cross reference data to merge all datasets listed into a single output feature class.  The input feature classes are re-projected into a common projection system before the results are merged. The data source path will be retained in a column/field named 'merged_src'.           

##### Python:
```python
dw.mergeFeatOnCrossRef(
                        crosRef,        # input cross reference file path
                        sr,             # arcpy spatial ref object
                        out_fc,         # output feature class path
                        tgt_schema)     # Optional, schema to match
```
