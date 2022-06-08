#build the dockerfile and download the required documents

make:
	wget https://s3.amazonaws.com/products-42matters/test/biographies.list.gz -O data/biographies.list.gz && \
	wget https://raw.githubusercontent.com/WittmannF/imdb-tv-ratings/master/top-250-movie-ratings.csv  -O data/top-250-movie-ratings.csv && \
	docker build -t pyspark:latest -f Dockerfile .

wordCount:
	docker run --rm -v $(shell /bin/pwd):/app  pyspark python src/2-wordCount.py

notebook:
	docker run --rm -p 8890:8888  -v $(shell /bin/pwd):/app  pyspark jupyter notebook --allow-root --no-browser --ip 0.0.0.0 --NotebookApp.token='' --NotebookApp.password=''

movieViews:
	docker run --rm -v $(shell /bin/pwd):/app  pyspark python src/3-movieViewEstimation.py

clean:
	rm output/*
	rm data/*
	docker rmi -f pyspark
