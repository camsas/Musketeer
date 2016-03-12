      for (relation_t::const_iterator l_it = rel_{{LEFT_REL}}.begin();
           l_it != rel_{{LEFT_REL}}.end();
           ++l_it) {
        std::string* {{LEFT_REL}}_tmp = new std::string("L");
        for (int {{LEFT_REL}}_i = 0; {{LEFT_REL}}_i < l_it->size(); {{LEFT_REL}}_i++) {
          *{{LEFT_REL}}_tmp += " ";
          *{{LEFT_REL}}_tmp += l_it->at({{LEFT_REL}}_i);
        }
        VLOG(3) << "emit from map (L): " << *{{LEFT_REL}}_tmp;
        map_emit((void *)l_it->at({{LEFT_INDEX}}),
                 (void *){{LEFT_REL}}_tmp->c_str(), {{LEFT_REL}}_tmp->length());
      }
      for (relation_t::const_iterator l_it = rel_{{RIGHT_REL}}.begin();
           l_it != rel_{{RIGHT_REL}}.end();
           ++l_it) {
        std::string* {{RIGHT_REL}}_tmp = new std::string("R");
        for (int {{RIGHT_REL}}_i = 0; {{RIGHT_REL}}_i < l_it->size(); {{RIGHT_REL}}_i++) {
          *{{RIGHT_REL}}_tmp += " ";
          *{{RIGHT_REL}}_tmp += l_it->at({{RIGHT_REL}}_i);
        }
        VLOG(3) << "emit from map (R): " << *{{RIGHT_REL}}_tmp;
        map_emit((void *)l_it->at({{RIGHT_INDEX}}),
                 (void *){{RIGHT_REL}}_tmp->c_str(), {{RIGHT_REL}}_tmp->length());
      }

