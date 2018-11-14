FROM python:3.4
ENV cwd /stock
ADD . ${cwd}
WORKDIR ${cwd}
RUN pip install -r requirements.txt
ENTRYPOINT ["python"]
CMD ["consumer.py"]