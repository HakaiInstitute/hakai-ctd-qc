# First step is to create a "build" container which installs all the dependencies and compresses them using `conda-pack` and copies them over to a smaller container image called "runtime"
# We do that because the time taken to build both containers from scratch each time adds up to 350s currently - that is too long
# Docker can cache the "build" image and not rebuild it each time because it doesn't change often
# However, our source code "./src" does change frequent as we update it. Therefore we cannot cache it.
FROM --platform=linux/amd64 continuumio/miniconda3:latest as build
RUN apt-get update
RUN apt-get install -y --no-install-recommends \
    build-essential gcc git

# Only copy the Conda environment file to install the dependencies
ADD docker-environment.yaml .

# Create the environment:
RUN conda init bash
RUN conda env create -f docker-environment.yaml

# Try install gsw from source on arm64
SHELL ["conda", "run", "-n", "hakai_qc", "/bin/bash", "-c"]
RUN git clone https://github.com/TEOS-10/GSW-Python.git
RUN cd GSW-Python && pip install .

# Install conda-pack:
RUN conda install -c conda-forge conda-pack

# Use conda-pack to create a standalone environment
# in /venv:
# Our environment is named "sensor_network"; see the `environment.yml` file
RUN conda-pack --name hakai_qc --output /tmp/env.tar && \
    mkdir /venv && cd /venv && tar xf /tmp/env.tar && \
    rm /tmp/env.tar

# We've put venv in same path it'll be in final image,
# so now fix up paths:
RUN /venv/bin/conda-unpack

# The runtime-stage image; we can use Debian as the
# base image since the Conda env also includes Python
# for us.
FROM --platform=linux/amd64  python:3.9.13-buster AS runtime

# Copy /venv from the previous stage:
COPY --from=build /venv /venv

# Add our project code and copy docker.env as .env
ADD . /venv


# When image is run, run the code within the Python virtual environment "venv"
SHELL ["/bin/bash", "-c"]
ENTRYPOINT source /venv/bin/activate && \
    python /venv/hakai_profile_qc  --update_qc --qc_unqced_profiles