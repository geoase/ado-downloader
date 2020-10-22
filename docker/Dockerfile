FROM python:3.8-slim-buster

# copy the module to the working directory
COPY . /app
WORKDIR /app

# install dependencies
RUN pip install -r requirements.txt

RUN python setup.py install

# command to run on container start
CMD [ "python", "./docker/entry.py" ]
