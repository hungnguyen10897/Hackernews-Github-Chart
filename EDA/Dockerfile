FROM jupyter/base-notebook

USER jovyan

# Install Pandas, pandas-gbq
RUN conda install --quiet --yes \    
    'pandas' \
    'pandas-gbq' --channel conda-forge
    
