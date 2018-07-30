
## Fusion Detection Reflow

*August 2018*
*Authors: * Lincoln Harris & Gerry Meixiong

Collection of scripts for detecting fusion transcripts from scRNA-seq data. Hoping to add reflow functionality. 

**Current Workflow:**
1. `python3 s3_crawler.py cellNamesFile.csv` where cellNamesFile contains all of your cells of interest, from an s3://czbiohub-seqbot run directory. Pulls down .fastqs for all of your cells of interest, sets up nested directory structure. 
2. `./fusionPipeline.sh` sets up BLAST databases for each cell, in parallel. Need a /querySeqs/ directory that contains .fa s for all of your genes of interest
3. `python3 findChimericReads.py` searches for chimeric reads from BLAST out files. 


