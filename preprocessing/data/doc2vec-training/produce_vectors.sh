training_file=$1
training_file_doc_only=$2
training_file_id_only=$3
result_vectors=$4


cut -f2 <$training_file >$training_file_doc_only
cut -f1 <$training_file >$training_file_id_only

rm doc2vecc
gcc doc2vecc.c -o doc2vecc -lm -pthread -O3 -march=native -funroll-loops

# this script trains on all the data (train/test/unsup), you could also remove the test documents from the learning of word/document representation
time ./doc2vecc -train $training_file_doc_only -word ./result/wordvectors.txt -output ./result/$result_vectors -cbow 1 -size 100 -window 10 -negative 5 -hs 0 -sample 0 -threads 4 -binary 0 -iter 20 -min-count 10 -test $training_file_doc_only -sentence-sample 0.1 -save-vocab ./result/alldata.vocab
sed -i '/^[0\. ]*$/d' ./result/$result_vectors #removes wrong vectors (consist only of 0)