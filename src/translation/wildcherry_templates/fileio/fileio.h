// CamIO 2: fileio.h
// Copyright (C) 2013: Matthew P. Grosvenor (matthew.grosvenor@cl.cam.ac.uk) 
// Licensed under BSD 3 Clause, please see LICENSE for more details. 

#ifndef FILEIO_H_
#define FILEIO_H_

#include "libchaste/include/chaste.h"

struct file_state;
typedef struct file_state file_state_t;

struct file_state {
    void* priv;

    ch_byte* (*read)(file_state_t* fs, ch_word* len_out);
    ch_word (*write)(file_state_t* fs, ch_byte* data, ch_word len);
    void (*delete)(file_state_t* fs);
} ;

file_state_t* fileio_new(ch_cstr type, ch_cstr filename);

#endif /* FILEIO_H_ */
