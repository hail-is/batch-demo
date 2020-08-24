# Overview

Welcome to the Batch Workshop portion of the ATGU Welcome Workshop!
The goal is to perform a GWAS and then clump the association results
so as to only report the most significant, independent associations.
Thank you to Nik Baya for generating the dataset we're going to use today.

Resources:
- https://hail.is/docs/batch/tutorial.html
- https://hail.is/docs/batch/docker_resources.html
- https://hail.is/docs/batch/service.html

If you're really stuck: https://hail.is/docs/batch/cookbook/clumping.html

# Prerequisites

1. Make sure docker works on your local computer. See the installation directions
[here](https://docs.docker.com/get-docker/):

```
$ docker version
```

2. Make sure you've downloaded and installed the Google SDK: https://cloud.google.com/sdk/install

3. Make sure you've authenticated Docker to use the Google Container Registry:

```
$ gcloud auth configure-docker
```

4. Follow the [installation directions](https://hail.is/docs/batch/getting_started.html)
to install Batch.

5. Download the zip file containing the code and example data and unzip it.

```
$ wget https://github.com/hail-is/batch-demo/archive/master.zip
$ unzip master.zip
$ cd batch-demo-master/
```

6. Open the `batch-demo-master` directory in a text editor such as Sublime, VSCode, emacs or vim.


# Run LD-clumping in local mode

1. Take a look at the *gwas_hail.py* script. It is using Hail to QC a dataset, compute association
test statistics, and export the data as a [binary PLINK file](https://zzz.bwh.harvard.edu/plink/data.shtml#bed).

2. Pull the hailgenetics/hail:0.2.37 image locally

```
$ docker pull hailgenetics/hail:0.2.37
```

3. Take a look at the *Dockerfile*. Follow the directions to fill in the file appropriately.

4. Build your image.

```
$ docker build -t batch-demo -f Dockerfile .
```

5. Verify your *gwas_hail.py* script is in the right location in your image and you can run `plink`.

```
$ docker run --rm -it batch-demo /bin/bash
```

6. Take a look at *demo.py*. Follow the directions to fill in the file appropriately.

7. Run the LD-clumping pipeline locally on a single chromosome

```
$ python3 demo.py \
  --local \
  --vcf example.chr21.vcf.bgz \
  --phenotypes sim_pheno.h2_0.8.pi_0.01.tsv \
  --output-file example.chr21.clumped \
  --chr 21
```

# Run LD-clumping on the Service

1. Create a bucket in the 'atgu-training' Google Cloud project called `batch-tmp-<USERNAME>`.
Feel free to use a different bucket, but for the purposes of this workshop, we'll assume you've
created one at the location above.

```
$ gcloud config set project atgu-training
$ gsutil mb gs://batch-tmp-<USERNAME>
```

2. Make sure you can go to [batch.hail.is](https://batch.hail.is).


3. Use hailctl to determine the name of your Hail Google service account. If you don't have `jq` installed,
you can also click on your username in the upper right corner of [batch.hail.is](https://batch.hail.is) or
just print out the results of `hailctl auth user` and copy and paste the gsa_email.

```
$ hailctl auth user | jq -r '.gsa_email'
```

4. Give your Hail Google service account permission to read / write files in your bucket.
SERVICE_ACCOUNT_NAME is the service account name from step 3 and the bucket name is the bucket
you created in step 1.

```
$ gsutil iam ch serviceAccount:[SERVICE_ACCOUNT_NAME]:objectAdmin gs://[BUCKET_NAME]
```

5. Give your Hail Google service account permission to read files in the bucket where the data
for the workshop are stored. SERVICE_ACCOUNT_NAME is the service account name from step 3.

```
$ gsutil iam ch serviceAccount:[SERVICE_ACCOUNT_NAME]:objectViewer gs://batch-ld-clumping-workshop
```

6. Give your Hail Google service account permission to upload Docker images to the Google
Container Registry (GCR). SERVICE_ACCOUNT_NAME is the service account name from step 3.

```
$ gsutil iam ch serviceAccount:[SERVICE_ACCOUNT_NAME]:objectViewer gs://artifacts.atgu-training.appspot.com
```

7. Push your Docker image that you created locally to GCR. Fill in USERNAME with your username.

```
$ docker tag batch-demo gcr.io/atgu-training/batch-demo-[USERNAME]
$ docker push gcr.io/atgu-training/batch-demo-[USERNAME]
```

8. Fill in the name of your Docker image that you pushed to GCR in the *demo.py* script. Also, fill in the
name of your bucket where you want temporary files to be generated (BUCKET_NAME) and the name of the billing
project 'atgu-welcome-workshop'.

```python3
    if args.local:
        BATCH_DEMO_IMAGE = 'batch-demo:latest'
        backend = hb.LocalBackend()
    else:
        # TODO: Fill in the location of your demo image in GCR
        # Fill this in when running LD-clumping on the service
        # This should look something like gcr.io/atgu-training/batch-demo-<user>:latest
        BATCH_DEMO_IMAGE = ...

    	# TODO: Fill in the name of <BILLING_PROJECT> and <YOUR_BUCKET>
        # Fill this in when running LD-clumping on the service	
    	# The billing project for the workshop is 'atgu-welcome-workshop'.
    	# The bucket is the name of the bucket that you configured your service account to have access to. Do not include the gs://
    	# In the future, you can use hailctl config to set defaults for these parameters
    	# `hailctl config set batch/billing_project my-billing-project`
    	# `hailctl config set batch/bucket my-bucket
        backend = hb.ServiceBackend(billing_project=<BILLING_PROJECT>,
                                    bucket=<BUCKET_NAME>)
```

9. Run *demo.py* omitting the `--local` flag to execute the batch on the Hail Service. Fill in BUCKET_NAME with your bucket that you
created in step 1.

```
$ python3 demo.py \
  --vcf gs://batch-ld-clumping-workshop/example.vcf.bgz \
  --phenotypes gs://batch-ld-clumping-workshop/sim_pheno.h2_0.8.pi_0.01.tsv \
  --output-file gs://[BUCKET_NAME]/example.chr21.clumped \
  --chr 21
```

10. Take a look at the Batch UI website that is printed out. Once the batch from step 9 finishes successfully, run the pipeline
on the complete dataset.

```
$ python3 demo.py \
  --vcf gs://batch-ld-clumping-workshop/example.vcf.bgz \
  --phenotypes gs://batch-ld-clumping-workshop/sim_pheno.h2_0.8.pi_0.01.tsv \
  --output-file gs://[BUCKET_NAME]/example.clumped
```

11. When the batch has finished, copy the clumped results to your local computer and take a look.

```
$ gsutil cp gs://[BUCKET_NAME]/example.clumped ./
```

12. Congratulations you've successfully run an LD-clumping pipeline with Batch using the Hail Service.