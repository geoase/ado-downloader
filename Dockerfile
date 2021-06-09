FROM python:3.8-slim-buster

# Create a user to run our app
RUN useradd --create-home appuser
WORKDIR /home/appuser

# install dependencies
COPY [".", "/home/appuser"]
RUN pip install -r requirements_linux.txt

# Install downloader
RUN python setup.py install

# Execution of app with appuser
USER appuser

# command to run on container start
#CMD [ "python", "../scripts/run_cds.py" ]
#ENTRYPOINT ["python","main.py"]
