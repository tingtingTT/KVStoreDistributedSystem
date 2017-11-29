FROM python:2

RUN mkdir /app
WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY . .

LABEL maintainer="CMPS128 team unicore HW#3" \
	  version="1.0"

CMD ["python", "app.py"]
