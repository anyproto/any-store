.PHONY: build clean

DIST := dist

build:
	mkdir -p $(DIST)
	GOOS=js GOARCH=wasm go build -o $(DIST)/anystore.wasm .
	cp "$$(go env GOROOT)/lib/wasm/wasm_exec.js" $(DIST)/
	cp js/loader.js $(DIST)/
	gzip -9 -k -f $(DIST)/anystore.wasm

clean:
	rm -rf $(DIST)
