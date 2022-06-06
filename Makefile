#build the dockerfile and download the required documents

make:
	wget https://s3.amazonaws.com/products-42matters/test/biographies.list.gz -O data/biographies.list.gz && \
	wget https://github.com/WittmannF/imdb-tv-ratings/blob/master/top-250-movie-ratings.csv  -O data/top-250-movie-ratings.csv && \
	docker build -t pyspark:latest -f Dockerfile .


clean:
	rm output/*
	rm data/*
	docker rmi -f pyspark
