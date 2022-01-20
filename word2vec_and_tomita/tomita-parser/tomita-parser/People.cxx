#encoding "utf8"

PersonName -> Word<kwtype = "ФИО">;
Person -> PersonName interp (Person.Name);
