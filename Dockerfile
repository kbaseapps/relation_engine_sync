FROM python:3.7-alpine

ARG DEVELOPMENT

# Install pip dependencies
RUN apk --update add --virtual build-dependencies python-dev build-base && \
    pip install --upgrade --no-cache-dir --extra-index-url https://pypi.anaconda.org/kbase/simple \
      kbase_module>0.0.2 \
      flake8 && \
    apk del build-dependencies

# Run the app
WORKDIR /kb/module
COPY . /kb/module
RUN chmod -R a+rw /kb/module
EXPOSE 5000
ENTRYPOINT ["sh", "/usr/local/bin/entrypoint.sh"]  # from the kbase_module package
