FROM public.ecr.aws/lambda/python:3.9

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY config/ ./config/
COPY src/ ./src/
COPY main.py .

CMD ["main.main"]