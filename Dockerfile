FROM python:3.7-slim

ARG DEVELOPMENT

# Install pip dependencies
COPY . /kb/module
RUN chmod -R a+rw /kb/module

# Install deps and run the app
WORKDIR /kb/module
RUN pip install --upgrade --no-cache-dir pip -r requirements.txt && \
    if [ "$DEVELOPMENT" ]; then pip install --no-cache-dir -r dev-requirements.txt; fi
ENTRYPOINT ["sh", "/kb/module/scripts/entrypoint.sh"]
