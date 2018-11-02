#ifndef LZF_H
#define LZF_H

#define LZF_VERSION 0X0105


unsigned int lzf_compress(const void *const in_data, unsigned int in_len, void *out_data, unsigned int out_len);


unsigned int lzf_decompress(const void *const in_data, unsigned int in_len, void *out_data, unsigned int out_len);

#endif
