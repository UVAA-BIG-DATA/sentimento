FROM python:3.6
ENV cwd /code
ADD . ${cwd}
WORKDIR ${cwd}
RUN pip install -r requirements.txt
ENTRYPOINT ["python"]
CMD ["main.py"]
