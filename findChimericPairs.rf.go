param (
	results_bucket string
)

val pair_script = file("./findChimericReads.py")

func findChimericPairs(cellBlast dir) dir =
	exec(image := "python") (output dir) {"
		python {{pair_script}} {{cellBlast}} {{output}}
	"}

// Save chimeric pairs to results_bucket/chimerPairFiles.
@requires(cpu := 20, mem := 64*GiB, disk := 200*GiB)
val Main = dirs.Copy(findChimericPairs(dir(strings.Join(["s3:/", results_bucket, "blastn"], "/"))), strings.Join(["s3:/", results_bucket, "chimerPairFiles"], "/"))