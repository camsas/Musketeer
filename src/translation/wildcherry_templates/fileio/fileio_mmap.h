// CamIO 2: fileio_mmap.h
// Copyright (C) 2013: Matthew P. Grosvenor (matthew.grosvenor@cl.cam.ac.uk) 
// Licensed under BSD 3 Clause, please see LICENSE for more details. 

#ifndef FILEIO_MMAP_H_
#define FILEIO_MMAP_H_

#include "fileio.h"

file_state_t* fileio_new_mmap(char* filename);

typedef struct {
    file_state_t* this;
    int fd;
    ch_word size;
    ch_byte* buff;
    ch_word bytes_read;
} file_state_mmap_t;


#endif /* FILEIO_MMAP_H_ */
