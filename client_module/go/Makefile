.PHONY: build clean
build:
	rm -f go pkg/go
	go build -o smartclient .
	@ls -la smartclient && echo "-> $(CURDIR)/smartclient"
clean:
	rm -f go smartclient
