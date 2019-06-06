select * from zephir_identifier_records zir
join zephir_identifiers zi on zir.identifier_autoid = zi.autoid
join zephir_records zr on zr.autoid = zir.record_autoid
where (type='oclc' and identifier in ('10115627', '10123266'))
or (type = 'contrib_sys_id' and identifier= 'nrlfGLAD307998-B')
order by zi.type desc, cid;

