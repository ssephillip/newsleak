
cut -f2 <doc2vec_train_with_id.txt >doc2vec_train.txt
cut -f1 <doc2vec_train_with_id.txt >doc2vec_id.txt

rm doc2vecc
gcc doc2vecc.c -o doc2vecc -lm -pthread -O3 -march=native -funroll-loops

# this script trains on all the data (train/test/unsup), you could also remove the test documents from the learning of word/document representation
time ./doc2vecc -train doc2vec_train.txt -word wordvectors.txt -output docvectors.txt -cbow 1 -size 100 -window 10 -negative 5 -hs 0 -sample 0 -threads 4 -binary 0 -iter 20 -min-count 10 -test doc2vec_train.txt -sentence-sample 0.1 -save-vocab alldata.vocab
sed -i '/^[0\. ]*$/d' docvectors.txt #removes wrong vectors (consist only of 0)