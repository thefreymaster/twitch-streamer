ARG BUILD_FROM
FROM $BUILD_FROM

RUN apk add --no-cache ffmpeg python3 py3-pip \
    && pip3 install --no-cache-dir --break-system-packages aiohttp

COPY controller.py /controller.py

CMD ["python3", "/controller.py"]
