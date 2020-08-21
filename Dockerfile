# TODO: specify the base image we're building on top of (hailgenetics/hail:0.2.37)
FROM ...


# --------------------------------------------------------

# Install curl from the linux package repository
## We remove the files from /var/lib/apt/lists/* to make the image smaller by removing temp files

RUN apt-get update && \
  apt-get -y install \
    curl && \
  rm -rf /var/lib/apt/lists/*

# --------------------------------------------------------

# Install PLINK in the image
## We make sure the plink binary is in a place on the path in /bin/

RUN curl -LO http://s3.amazonaws.com/plink1-assets/plink_linux_x86_64_latest.zip && \
    unzip plink_linux_x86_64_latest.zip && \
    mv plink /bin/ && \
    rm -rf plink_linux_x86_64_latest.zip

# --------------------------------------------------------

# TODO: copy the script that performs the GWAS in Hail `gwas_hail.py` into the image
## The source is relative to the context path when you run `docker build`
## The dest is where you want the file to be found in your Docker image.

COPY ... ...
