FROM envoyproxy/envoy:v1.14.1
CMD /usr/local/bin/envoy -c /etc/envoy.yaml
