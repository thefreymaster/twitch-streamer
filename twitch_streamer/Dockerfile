ARG BUILD_FROM
FROM $BUILD_FROM

# chromium + xvfb render the Bambuddy overlay; ffmpeg grabs the display.
RUN apk add --no-cache ffmpeg python3 py3-pip chromium xvfb font-dejavu \
    && pip3 install --no-cache-dir --break-system-packages aiohttp

COPY controller.py /controller.py
COPY run.sh /run.sh
RUN chmod a+x /run.sh

CMD ["/run.sh"]
