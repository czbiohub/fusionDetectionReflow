
## Fusion Detection Reflow

*Date:* August 2018               
*Authors:* Lincoln Harris & Gerry Meixiong                   

Collection of scripts for detecting fusion transcripts from scRNA-seq data. Hoping to add REFLOW functionality. 

**Current Workflow:**
1. `python3 s3_crawler.py prefixList.csv` where *prefixList.csv* contains a list of *s3://czbiohub-seqbot* directories to search through. Pulls down .fastqs for all of your cells of interest, sets up nested directory structure. 
2. `./fusionPipeline.sh` sets up BLAST databases for each cell, in parallel. Need a */querySeqs/* directory that contains .fa s for all of your genes of interest
3. `python3 findChimericReads.py` searches for chimeric reads from BLAST out files. 


