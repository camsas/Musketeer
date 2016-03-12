// CamIO 2: fileio_cwrite.h
// Copyright (C) 2013: Matthew P. Grosvenor (matthew.grosvenor@cl.cam.ac.uk) 
// Licensed under BSD 3 Clause, please see LICENSE for more details. 

#ifndef FILEIO_CWRITE_H_
#define FILEIO_CWRITE_H_

#include "fileio.h"

file_state_t* fileio_new_cwrite(char* filename);

typedef struct {
    file_state_t* this;
    int fd;
    ch_word size;
    ch_byte* buff;
    ch_word bytes_written;
} file_state_cwrite_t;


#endif /* FILEIO_CWRITE_H_ */
