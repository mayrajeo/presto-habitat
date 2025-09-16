# Running presto for downstream tasks

[Presto](https://github.com/nasaharvest/presto) is a promising foundation model for remote sensing data, originally created for crop classificatoin. This repo is an example workflow for

1. Creating the data suitable for the model, using CDSE and Paituli STAC
2. Sampling the datacubes with point locations
3. Running [Presto](https://www.youtube.com/watch?v=5-tqtgQFcHc) for habitat classification in Northern Lapland, Finland.

Source code in presto folder adapted from the original repository, as well as from [WorldCereal/presto-worldcereal](https://github.com/WorldCereal/presto-worldcereal/).

## Installation

Clone the repository and `conda env create -f env.yml`. For jupyter notebooks, either `conda install -c conda-forge notebook` or `conda install -c conda-forge ipykernel`, depending how your jupyter is set up.