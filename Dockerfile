FROM python:3.10-slim

WORKDIR /BELmc

COPY . /BELmc

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "collector_ems.py"]
