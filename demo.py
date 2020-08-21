import argparse
import re
import hailtop.batch as hb

from typing import List

def run_gwas(batch: hb.batch.Batch, image: str, vcf: hb.resource.ResourceFile, phenotypes: hb.resource.ResourceFile) -> hb.job.Job:
    """
    Get association test statistics
    Also, export PLINK file
    """

    # TODO: Take the input batch and create a new job object called `gwas`. You can give it the name 'run-gwas'
    # which will be useful when looking at the Batch UI to see what job corresponds to your code.
    gwas = ...

    # TODO: Tell the new `gwas` job the image name of where you pushed your image in GCR that contains your Hail script
    # This image name is defined by the `image` variable as an argument to the function    
    gwas.image(...)
    
    # This is how we tell Batch that we want this job to have 4 cores. This number must match the argument
    # to `gwas_hail.py`, which tells Hail to run in local mode with 4 cores available.
    gwas.cpu(4)

    # This is how we tell Batch that we're defining a new ResourceGroup that is the output of the `gwas` Job.
    # PLINK will output four files here with a common root name. We designate this common file root with `{root}`
    # and hard code the extensions pertaining to each file. Now we can reference the common file root as `gwas.ofile`
    # To reference the bim file specifically, we can use `gwas.ofile.bim` or `gwas.ofile['bim']`
    gwas.declare_resource_group(ofile={
        'bed': '{root}.bed',
        'bim': '{root}.bim',
        'fam': '{root}.fam',
        'assoc': '{root}.assoc'
    })

    # The command definition below uses f-strings. The contents in between curly braces ({, }) are evaluated as Python expressions.
    # TODO: Fill in the <PATH> to the Python script `gwas_hail.py` with its location in the Docker image specified above.
    # TODO: Fill in the argument <VCF> which represents the VCF file we passed in to the function above.
    # TODO: Fill in the argument <OUTPUT_FILE> to the `--output-file` argument below. This should be the file root of the resource group declared above.
    gwas.command(f'''
python3 <PATH> \
    --vcf <VCF> \
    --phenotypes {phenotypes} \
    --output-file <OUTPUT_FILE> \
    --cores 4
''')

    # We return the `gwas` Job object that can be used in downstream jobs.
    return gwas


def clump(batch: hb.batch.Batch, image: str, bfile: hb.resource.ResourceGroup, assoc: hb.resource.ResourceFile, chr: int) -> hb.job.Job:
    """
    Clump association results with PLINK.

    https://zzz.bwh.harvard.edu/plink/clump.shtml
    """

    # Define a new job `c` in Batch `batch` with name `clump-CHR`
    c = batch.new_job(name=f'clump-{chr}')

    # This image name is defined by the `image` variable as an argument to the function
    c.image(image)

    # Tell Batch to use 1Gi of memory for this job
    c.memory('1Gi')

    # Tell Batch to use 1 cpu for this job
    c.cpu(1)
    
    # Notice that we can simply call plink2 here because we put it on the PATH in the Dockerfile
    # TODO: Fill in the argument <BFILE> which uses the `bfile` argument we passed in to the function above.
    # `bfile` is a resource group and is expected to have three files at a common root name: {root}.bed, {root}.bim, {root}.fam
    # TODO: Fill in the argument <ASSOC> which uses the `assoc` argument we passed in to the function above. This file has the p-values of the GWAS.
    # TODO: Fill in the argument <CHR> which uses the `chr` we passed in to the function above. This will tell PLINK to only compute
    # the clumping results for this chromosome. This is how we achieve parallelism by chromosome.
    c.command(f'''
    plink --bfile <BFILE> \
    --clump <ASSOC> \
    --chr <CHR> \
    --clump-p1 0.0001 \
    --clump-p2 0.001 \
    --clump-r2 0.5 \
    --clump-kb 1000 \
    --memory 1024 \
    --threads 1

mv plink.clumped {c.clumped} || \
echo " CHR    F              SNP         BP        P    TOTAL   NSIG    S05    S01   S001  S0001    SP2" > {c.clumped}
''')

    # PLINK outputs the results at a hardcoded path. So we'll move it to a path Batch will know to copy.
    # PLINK doesn't output a file if there are no results so we'll make one

    # We return the `c` Job object that can be used in downstream jobs.
    return c


def merge(batch: hb.batch.Batch, results: List[hb.resource.ResourceFile]) -> hb.job.Job:
    """
    Merge clumped results files together
    """

    # Define a new job `merger` in Batch `batch` with name `merge-results`
    merger = batch.new_job(name='merge-results')

    # Use the ubuntu:18.04 image which Batch caches
    merger.image('ubuntu:18.04')

    # Do some file munging to concatenate all of the clumped results together for all chromosomes
    if results:
        merger.command(f'''
head -n 1 {results[0]} > {merger.ofile}
for result in {" ".join(results)}
do
    tail -n +2 "$result" >> {merger.ofile}
done
sed -i '/^$/d' {merger.ofile}
''')

    # We return the `merger` Job object that can be used in downstream jobs.
    return merger


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--vcf', required=True)
    parser.add_argument('--phenotypes', required=True)
    parser.add_argument('--output-file', required=True)
    parser.add_argument('--local', action='store_true')
    parser.add_argument('--chr', default='1-22')

    args = parser.parse_args()

    chromosomes = []
    for chr_arg in args.chr.split(','):
        if re.match('^\d+$', chr_arg):
            chromosomes.append(int(chr_arg))
            continue
        
        match = re.match('^(\d+)-(\d+)$', chr_arg)
        if match:
            start, end = match.groups()
            chromosomes.extend(list(range(int(start), int(end) + 1)))
        else:
            raise NotImplementedError(chr_arg)

    if args.local:
        BATCH_DEMO_IMAGE = 'batch-demo:latest'
        backend = hb.LocalBackend()
    else:
        # TODO: Fill in the location of your demo image in GCR
        # Fill this in when running LD-clumping on the service
        # This should look something like gcr.io/atgu-training/batch-demo-<user>:latest
        BATCH_DEMO_IMAGE = ...

        # TODO: Fill in the name of <YOUR_BILLING_PROJECT> and <YOUR_BUCKET>
        # Fill this in when running LD-clumping on the service        
        # The billing project for the workshop is 'atgu-welcome-workshop'.
        # The bucket is the name of the bucket that you configured your service account to have access to. Do not include the gs://
        # In the future, you can use hailctl config to set defaults for these parameters
        # `hailctl config set batch/billing_project my-billing-project`
        # `hailctl config set batch/bucket my-bucket        
        backend = hb.ServiceBackend(billing_project=...,
                                    bucket=...)

    batch = hb.Batch(backend=backend,
                     name='clumping-demo')
    # Define inputs
    vcf = batch.read_input(args.vcf)
    # TODO: We want to read the input file for the phenotypes and make it an InputResourceFile
    # look at the vcf file above for an example of creating an InputResourceFile. The phenotypes
    # file is passed as `args.phenotypes`
    phenotypes = ...

    # QC and compute gwas assoc results
    # TODO: Fill in the argument parameters to the `run_gwas` function
    # This will add a new job to the Batch `batch` that runs a GWAS in Hail
    # and exports the dataset to PLINK format. It also takes as arguments the batch to use, the name
    # of the Docker image to use, a VCF file and a file with the phenotypes.
    gwas = run_gwas(..., BATCH_DEMO_IMAGE, ..., ...)

    # Run PLINK clumping once per chromosome
    results = []
    for chr in chromosomes:
        # TODO: Fill in the argument parameters to the `clump` function
        # This will add a new job to the Batch `batch` that clumps the p-values for a given chromosome
        # It also takes as arguments the batch to use, the name of the Docker image to use, a PLINK binary file (where should this come from?),
        # a file with the association results (where should this come from?) and the chromosome to use.
        c = clump(..., ..., ..., ..., ...)

        # We add the clumped results file (a ResourceFile object) to a list of results to use in the merge step later.
        results.append(c.clumped)

    # Merge clumping results together
    # TODO: Fill in the argument parameters to the `merge` function
    # This will add a new job to the Batch `batch` that merges all the clumped results together into one file.
    # The arguments are the batch to use and the list of clumped result files to concatenate.
    m = merge(..., ...)

    # Write output file with clumped results
    # TODO: Fill in the argument parameters to be able to save the merged results file to a final location
    # specified by `args.output_file`. Note that all files produced by Batch are temporary unless you specifically
    # write the output to a permanent location.
    batch.write_output(..., ...)

    if args.local:
        batch.run(verbose=True, delete_scratch_on_exit=False)
    else:
        batch.run(open=True, wait=False)
        backend.close()
