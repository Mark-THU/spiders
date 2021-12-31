# bloom_filter
* Param
```
name: name of BF, to save bitarray
length: length of bitarray
number: number of hash functions
save_frequency: frequency to save bitarray
```  
* Attention
```
number of hash functions can't exceed 11, and must more than 0.
a file with name param(name) will be created at homework space.
```
* Functions
```
is_contain: to judge whether a key is in BF, return true/false
insert: insert a key into BF
```
