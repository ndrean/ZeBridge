FROM dart:stable
ARG ZB_VERSION
WORKDIR /smoke
COPY pubspec.yaml ./
RUN sed -i "s/^  zebridge: .*/  zebridge: ${ZB_VERSION}/" pubspec.yaml && grep -q "zebridge: ${ZB_VERSION}$" pubspec.yaml
COPY bin bin
RUN dart pub get
CMD ["dart", "run", "bin/smoke.dart"]
