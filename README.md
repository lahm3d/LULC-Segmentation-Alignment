# LULC-Segmentation-Alignment
Workflow to dissolve and align segments

To execute:
`C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python dissolve_and_align_segments.py -batch batch.csv`

Batch csv contains paths for all the files the script needs to execute. You can add multiple counties here has well. It should be formatted like this:

| segs | o_segs | lc_raw | lc_albers | aligned_segs |
| --------------- | --------------- | --------------- |--------------- |---------------|
| C:\...\balt_24005_landcover_2018_draft_spf | C:\...\balt_24005_landcover_2018_draft_spf_diss | C:\...\balt_24005_landcover_2018_spf.img | C:\...\balt_24005_lc_2018_project_test.img |C:\...\aligned_segs |
| C:\...\balt_24005_landcover_2018_draft_spf | C:\...\balt_24005_landcover_2018_draft_spf_diss | C:\...\balt_24005_landcover_2018_spf.img | C:\...\balt_24005_lc_2018_project_test.img |C:\...\aligned_segs |
| C:\...\balt_24005_landcover_2018_draft_spf | C:\...\balt_24005_landcover_2018_draft_spf_diss | C:\...\balt_24005_landcover_2018_spf.img | C:\...\balt_24005_lc_2018_project_test.img |C:\...\aligned_segs |
