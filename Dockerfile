FROM python:3

RUN git clone https://github.com/ozop/gargantua.git

RUN pip install --no-cache-dir -r /gargantua/requirements.txt

CMD [ "python", "/gargantua/gargantua.py" ]