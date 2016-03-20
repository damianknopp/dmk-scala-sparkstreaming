# dmk-scala-sparkstreaming



 
To build
---
cd $SPARK_HOME
dmk-scala-sparkstreaming/helper-scripts/build-and-deploy.sh

To run
---

	./dmk-run-fstreamtest.sh baseball-in/ baseball-out/
	cp tmp-data-split/x?? baseball-in/.

	./dmk-run-flumetreamtest.sh localhost:9905

	./dmk-run-simplefpgrowth.sh belgium-retail.dat 
  * Data from Belgium retail market dataset  
  * http://www.recsyswiki.com/wiki/Grocery_shopping_datasets and http://fimi.ua.ac.be/data/retail.pdf

