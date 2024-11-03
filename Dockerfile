FROM python:3.11.5-alpine

WORKDIR /app
ADD ./requirements.txt /app/backend/

# RUN pip3 install --upgrade pip
RUN pip3 install gunicorn
RUN pip3 install -r /app/backend/requirements.txt

#RUN mkdir -p /app/LOGS
ADD ./wsgi-entrypoint.sh /app/docker/
ADD ./ /app/backend

RUN chmod +x /app/backend/manage.py
#RUN python3 /app/backend/manage.py makemigrations
#RUN python3 /app/backend/manage.py migrate
