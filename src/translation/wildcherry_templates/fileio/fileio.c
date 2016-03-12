// CamIO 2: fileio.c
// Copyright (C) 2013: Matthew P. Grosvenor (matthew.grosvenor@cl.cam.ac.uk) 
// Licensed under BSD 3 Clause, please see LICENSE for more details. 

#include "fileio.h"
#include "fileio_mmap.h"
#include "fileio_cwrite.h"

file_state_t* fileio_new(ch_cstr type, ch_cstr filename){
    if(!strcmp(type,"mmap")){
        return fileio_new_mmap(filename);
    }
    else if(!strcmp(type,"cwrite")){
        return fileio_new_cwrite(filename);
    }

    ch_log_fatal("Unkown filio type=%s\n    ", filename);
    return NULL;
}
