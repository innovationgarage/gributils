Service example for docker stack:

    indexer:
      image: innovationgarage/gributils
      environment:
        ESURL: "http://elasticsearch:9200"
      volumes:
        - gribdata:/data
      ports:
        - "1028:1028"
