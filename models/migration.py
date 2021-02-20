from odoo import models, fields, api
from odoo.addons.queue_job.job import job
import odoorpc
import logging
import json
from odoo.exceptions import ValidationError
_log = logging.getLogger(__name__)

def get_chunks(iterable, n=1000):
    for i in range(0, len(iterable), int(n)):
        yield iterable[i:i + int(n)]

# creating an user raises: 'You cannot create a new user from here.
#  To create new user please go to configuration panel.'

BASE_MODEL_PREFIX = ['ir.', 'mail.', 'base.', 'bus.', 'report.', 'account.', 'res.users', 'stock.location', 'res.',
                     'product.pricelist', 'product.product', 'product.template', 'stock.picking.type','uom.']
MODELS_WITH_EQUAL_IDS = ['res.partner', 'product.product', 'product.template', 'product.category', 'seller.instance']

class MigrationRecord(models.Model):
    _name = 'migration.record'
    name = fields.Char()
    model = fields.Char(index=True)
    old_id = fields.Integer(index=True)
    new_id = fields.Integer(index=True)
    company_id = fields.Many2one('res.company', related='migration_model.company_id')
    data = fields.Text(help='Old data in JSON format')
    state = fields.Selection([('pending', 'Pending'),('done','Done'),('error', 'Error'),('by_system', 'Created by sistem')])
    migration_model =  fields.Many2one('migration.model')
    state_message = fields.Text()
    type = fields.Char()
    relation = fields.Char()


    def map_records(self):
        model = self.migration_model.model or self.model
        company_id = self.company_id.id
        if not model:
            raise ValidationError('Model is required')
        #if self.data:
        #    data = json.loads(self.data)
        new_id = self.get_new_id(model, self.old_id, company_id=company_id, create=False)
        if new_id:
            return new_id
        name = self.name
        res_model = self.env[model]
        has_name = hasattr(res_model, 'name')
        has_complete_name = hasattr(res_model, 'complete_name')
        if (has_name or has_complete_name) and name:
            domain = [('complete_name' if has_complete_name else 'name', '=', name)]
            has_company = hasattr(res_model, 'company_id')
            if has_company and company_id:
                domain.append(('company_id', '=', company_id))

            new_rec = res_model.search(domain, limit=1).id
            if new_rec:
                self.write({'new_id': new_rec, 'model': model, 'state': 'done',})
                return new_rec


    def get_new_id(self, model, old_id, test=False, company_id=0, create=True):
        domain = [('model', '=', model), ('old_id', '=', old_id)]
        if company_id:
            domain.append(('company_id', '=', company_id))
        rec = self.search(domain, limit=1)
        if rec.new_id:
            return rec.new_id
        if rec.data and create:
            data = json.loads(rec.data)
            if rec.company_id and data.get('company_id'):
                data['company_id'] = rec.company_id.id
            return rec.get_or_create_new_id(data, field_type=rec.type, relation=model, test=test)
        return 0

    def prepare_vals(self, data={}, fields_mapping={}, model='', test=False):
        if not data and self.data:
            data = json.loads(self.data)
        if not fields_mapping and model:
            fields_mapping = self.env[model].fields_get()
        vals = {}
        for key in data:
            if key in ('id', 'display_name'):
                continue
            company_id = self.company_id or self.migration_model.company_id
            if key == 'company_id' and company_id:
                vals[key] = company_id.id
                continue
            value = data[key]
            if isinstance(value, (list, tuple)):
                field_map = fields_mapping.get(key) or {}
                field_type = field_map.get('type')
                if field_type == 'many2one':
                    # value is a tuple with (id, name)
                    try:
                        new_id = self.browse().get_or_create_new_id(value, field_map=field_map, test=test)
                        if new_id:
                            vals[key] = new_id
                    except Exception as e:
                        _log.exception(e)
                        if test:
                            self.env.cr.rollback()
                        else:
                            self.env.cr.commit()  # to avoid InFailedSqlTransaction transaction block

                elif field_type == 'one2many':
                    # in this case we have to import first the relation
                    pass
                elif field_type == 'many2many':
                    related = field_map.get('relation')
                    if not related:
                        continue
                    values = [self.browse().get_or_create_new_id(
                        value=[old, ''], relation=related, field_type=field_type, flag_try_old_id=False,
                        test=test, company_id=self.migration_model.company_id.id) for old in value]
                    values = [v for v in values if v]
                    if values:
                        vals[key] = [[6, 0, values]]

            else:
                # simple value, int, str
                vals[key] = value
        return vals

    def get_or_create_new_id(self, value=None, field_map=False,  field_type='', relation='', flag_try_old_id=False, test=False, company_id=0):
        """
        :param flag_try_old_id:
        :param field_map: dict with keys: name, type, required, relation, etc
        :param value: tuple(id, name) or dict
        :raises: Exeption if fail creating record
        :return int
        """
        company_id = company_id or self.migration_model.company_id.id
        migration_model = False
        if self.new_id:
            return self.new_id
        if field_map:
            field_type = field_map.get('type')
            relation = field_map.get('relation')
        if not value:
            if self.data:
                value = json.loads(self.data)
            else:
                return 0
        if not relation:
            relation = self.model
        flag_try_old_id = flag_try_old_id or relation in MODELS_WITH_EQUAL_IDS
        res_model = self.env[relation]
        has_name = hasattr(res_model, 'name')
        has_complete_name = hasattr(res_model, 'complete_name')
        new_rec = 0
        old_id = self.old_id
        raw_vals = False
        name = False
        if isinstance(value, (list,tuple)) and len(value) == 2:
            old_id = value[0]
            name = value[1]
            id = self.get_new_id(relation, old_id, company_id=company_id)
            if id:
                return id
        elif field_map and isinstance(value, dict):
            old_id = value.get('id') or self.old_id
            name = value.get('name')
            raw_vals = value
            id = self.get_new_id(relation, old_id)
            if id:
                return id
        elif self.exists() and isinstance(value, dict):
            raw_vals = value
        if (has_name or has_complete_name) and name:
            domain = [('complete_name' if has_complete_name else 'name', '=', name)]
            has_company = hasattr(res_model, 'company_id')
            if has_company and company_id:
                domain.append(('company_id', '=', company_id))

            new_rec = res_model.search(domain, limit=1).id
        if not new_rec and flag_try_old_id:
            new_rec = res_model.browse(old_id).exists().id
        if not new_rec:
            omit = relation and [True for r in BASE_MODEL_PREFIX if relation.startswith(r)]
            if omit:
                _log.warning('try to create a base model record %s for %s' % (relation, [old_id, name]))
                return 0
            if raw_vals:
                vals = self.prepare_vals(raw_vals, model=relation)
                new_rec = res_model.create(vals).id
            elif old_id:
                # fetch data from old server
                try:
                    migration_domain = [('model', '=', relation)]
                    if company_id:
                        migration_domain += ['|', ('company_id', '=', company_id), ('company_id', '=', False)]
                    migration_model = self.migration_model.search(migration_domain, limit=1)
                    if migration_model:
                        if not migration_model.old_fields_list:
                            migration_model.compute_fields_mapping()
                        fields_to_read = json.loads(migration_model.old_fields_list)
                        fields_to_read.append('display_name')
                        old_model = migration_model.conn().env[relation]
                        data = old_model.search_read([('id', '=', old_id)], fields_to_read)
                        if data:
                            vals = self.prepare_vals(data[0], model=relation)
                            new_rec = res_model.create(vals).id
                    elif name:
                        new_rec = res_model.create({'name': name}).id
                except Exception as e:
                    if test:
                        self.env.cr.rollback()
                    else:
                        self.env.cr.commit()
                    _log.exception(e)
        if new_rec and old_id:
            try:
                if self.exists():
                    self.write({'new_id': new_rec, 'model': relation, 'state': 'done', 'type': field_type,
                                'relation': relation})
                else:
                    vals_to_create = {'new_id': new_rec, 'model': relation, 'old_id': old_id, 'state': 'done', 'type': field_type, 'relation': relation}
                    if migration_model:
                        vals_to_create['migration_model'] = migration_model.id
                    self.create([vals_to_create])
            except Exception as e:
                if test:
                    self.env.cr.rollback()
                else:
                    self.env.cr.commit()
                self.write({'state': 'error', 'state_message': repr(e)})
        return new_rec


class MigrationCredentials(models.Model):
    _name = 'migration.credentials'
    database = fields.Char()
    url = fields.Char()
    port = fields.Char()
    user = fields.Char()
    password = fields.Char()
    protocol = fields.Selection([('jsonrpc', 'jsonrpc'),('jsonrpc+ssl', 'jsonrpc+ssl')], 'Protocol')


class MigrationModel(models.Model):
    _name = 'migration.model'
    _order = 'sequence'
    name = fields.Char()
    state = fields.Selection([
        ('draft','Draft'),
        ('to_fetch','To fetch'),
        ('fetching', 'Fetching Data'),
        ('ready', 'Ready to import'),
        ('importing', 'Importing Data'),
        ('done', 'Done'),
        ('error', 'Error')], default='draft')
    sequence = fields.Integer(default=1000)
    credentials_id = fields.Many2one('migration.credentials')
    model = fields.Char()
    company_id = fields.Many2one('res.company', help="company to import records")
    old_company_id = fields.Integer(help='the company id on the old server')
    date_from = fields.Date()
    date_to = fields.Date()
    record_states = fields.Char(help='state of records to migrate separated by ,')
    fields_mapping = fields.Text(
        stored=True,
        help='JSON object key:object where key is old field name and value the new one, add all fields you want to migrate')
    threads = fields.Integer(help='for parallel workers, if it\'s 0 will execute synchronous')
    migration_record_ids = fields.One2many('migration.record', 'migration_model')
    status_message = fields.Text(stored=True)
    total_records = fields.Integer()
    extra_domain = fields.Char(help='domain extra in json format [["field", "operator", "value"]]', default='[["active","=",true]]')

    max_deep_level = fields.Integer(help="limit for recursion", default=3)
    current_deep_level = fields.Integer(help="the current level for this model", default=1)
    parent_id = fields.Many2one('migration.model')
    relation_field = fields.Char()
    dependency_ids = fields.One2many('migration.model', 'parent_id', ondelete='cascade')
    omit_fields = fields.Char(help="fields to omit separated by ,")
    old_fields_list = fields.Text()
    # Computed fields
    fetch_records = fields.Integer(compute='_compute_progress')
    fetch_progress = fields.Integer(compute='_compute_progress')
    migrated_records = fields.Integer(compute='_compute_progress')
    migration_progress = fields.Integer(compute='_compute_progress')

    def compute_fields_mapping(self, dependencies=[]):
        for rec in self:
            try:
                omit_fields = []
                if self.omit_fields:
                    omit_fields = self.omit_fields.split(',')
                conn = self.conn()
                res_model = self.env[rec.model]
                if res_model._transient:
                    return
                model_fields = res_model.fields_get()
                stored_fields = [f for f in model_fields if model_fields[f].get('store', True)]
                old_res_model = conn.env[rec.model]
                old_model_fields = old_res_model.fields_get()
                old_fields_list = []
                fields_mapping = {}
                if not dependencies:
                    dependencies = rec.dependency_ids.search(['|', ('company_id', '=', rec.company_id.id), ('company_id','=', False)]).mapped('model')
                for field in stored_fields:
                    if field in omit_fields:
                        continue
                    new_field = model_fields[field]
                    if new_field.get('type') == 'one2many':
                        continue
                    old_field = old_model_fields.get(field)
                    if old_field:
                        name = field
                        old_fields_list.append(field)
                    else:
                        name = None
                    relation = (new_field.get('relation') or '') if name else ''
                    recursive = new_field.get('type') in ('many2one',)
                    data = {
                        'name': name,
                        'type': new_field.get('type'),
                        'required': new_field.get('required'),
                        'relation': relation,
                        'relation_field': new_field.get('relation_field'),
                        'recursive': recursive,
                    }
                    omit = relation and [True for r in BASE_MODEL_PREFIX if relation.startswith(r)]
                    if omit:
                        continue
                    if relation and recursive and relation not in dependencies and relation != rec.model and rec.current_deep_level < rec.max_deep_level:
                        dep_vals = {
                            'name': relation,
                            'state': 'draft',
                            'company_id': rec.company_id.id,
                            'parent_id': rec.id,
                            'date_from': rec.date_from,
                            'date_to': rec.date_to,
                            'record_states': rec.record_states,
                            'sequence': rec.sequence - 1,
                            'model': relation,
                            'credentials_id': rec.credentials_id.id,
                            'old_company_id': rec.old_company_id,
                            'relation_field': data.get('relation_field'),
                            'max_deep_level': rec.max_deep_level,
                            'extra_domain': '[]',
                            'current_deep_level': rec.current_deep_level + 1,
                        }
                        dependencies.append(relation)
                        self.create(dep_vals).compute_fields_mapping()

                    fields_mapping[field] = data
                rec.fields_mapping = json.dumps(fields_mapping, indent=2)
                rec.old_fields_list = json.dumps(old_fields_list)
                rec.state = 'to_fetch'
            except Exception as e:
                rec.fields_mapping = ''
                rec.state = 'error'
                rec.status_message = repr(e)

    def _compute_progress(self):
        for rec in self:
            rec.fetch_records = len(rec.migration_record_ids)
            rec.fetch_progress = rec.total_records and (rec.fetch_records / rec.total_records) * 100
            rec.migrated_records = self.migration_record_ids.search_count([('migration_model', '=', rec.id), ('state', '=', 'done')])
            rec.migration_progress = rec.total_records and (rec.migrated_records / rec.total_records) * 100

    def conn(self):
        rpc_conn = odoorpc.ODOO(self.credentials_id.url, port=self.credentials_id.port, protocol=self.credentials_id.protocol)
        rpc_conn.login(self.credentials_id.database, self.credentials_id.user, self.credentials_id.password)
        return rpc_conn

    def set_draft(self):
        for rec in self:
            rec.state = 'draft'

    def run_test(self, show_confirmation=True):
        try:
            self.prepare_records_from_old_server(test=True)
            self.env.cr.rollback()
        except Exception as e:
            raise ValidationError('Test Failed\n %s'% repr(e))
        if show_confirmation:
            raise ValidationError('Test Success')

    def button_start(self):
        if self.threads:
            self.with_delay().prepare_records_from_old_server(run_import=True)
        else:
            try:
                self.prepare_records_from_old_server(run_import=True)
            except Exception as e:
                self.env.cr.rollback()
                self.state = 'error'
                self.status_message = repr(e)
    
    def run_import_process(self, test=False):
        if self.state != 'ready':
            return  # raise ValidationError('Fetch data is no ready')
        if test:
            self.run_import_batch(self.migration_record_ids, test=test)
        if self.threads > 0:
            self.state = 'importing'
            chunks = get_chunks(self.migration_record_ids, self.threads)
            for batch in chunks:
                self.with_delay().run_import_batch(batch)

            # run in jobs
        else:
            for batch in get_chunks(self.migration_record_ids, 500):
                self.run_import_batch(batch)
            self.state = 'done'

    @job
    def run_import_batch(self, migration_record_ids, test=False):
        fields_mapping = json.loads(self.fields_mapping)
        for rec in migration_record_ids.filtered(lambda r: not r.new_id):
            try:
                new_obj = rec.get_or_create_new_id(test=test)
            except Exception as e:
                if test:
                    self.env.cr.rollback()
                else:
                    self.env.cr.commit()  # to avoid InFailedSqlTransaction transaction block
                rec.state = 'error'
                rec.state_message = repr(e)
        if test:
            self.env.cr.rollback()
        else:
            self.env.cr.commit()  # to avoid InFailedSqlTransaction transaction block

    def map_records(self):
        if not self.migration_record_ids:
            self.prepare_records_from_old_server(run_import=False)
        for batch in get_chunks(self.migration_record_ids):
            for rec in batch:
                rec.map_records()
            self.env.cr.commit()


    @job
    def prepare_records_from_old_server(self, run_import=False, test=False):
        try:
            if self.state != 'to_fetch':
                # raise ValidationError('Migration state must be in To fetch')
                return
            self.state = 'fetching'
            if not test:
                self.env.cr.commit()
            limit = 100 if test else None
            conn = self.conn()
            new_model = self.env[self.model]
            old_model = conn.env[self.model]
            domain = []
            if self.old_company_id and hasattr(new_model, 'company_id'):
                domain.append(('company_id', '=', self.old_company_id))
            if self.record_states and hasattr(new_model, 'state'):
                states = self.record_states.split(',')
                domain.append(('state', 'in', states))
            if self.date_from:
                domain.append(('create_date', '>=', str(self.date_from)))
            if self.date_to:
                domain.append(('create_date', '<=', str(self.date_to)))
            if self.extra_domain:
                extra_domain = json.loads(self.extra_domain)
                domain += extra_domain
            fields_to_read = json.loads(self.old_fields_list)
            fields_to_read.append('display_name')
            old_records = old_model.search(domain, limit=limit)
            self.total_records = len(old_records)
            chunks = get_chunks(old_records)
            dependencies = self.dependency_ids.search([('state', '=', 'to_fetch'), ('id', '!=', self.id)])
            for dep in dependencies:
                if dep.state == 'to_fetch':
                    dep.prepare_records_from_old_server(run_import=False, test=test)
            for batch in chunks:
                data = old_model.search_read([('id', 'in', batch)], fields_to_read)
                self.migration_record_ids = [[0, 0, {
                    'old_id': d.get('id'),
                    'data': json.dumps(d),
                    'model': self.model,
                    'name': d.get('display_name'),
                    'state': 'pending',
                }] for d in data]
                if not test:
                    self.env.cr.commit()
            self.state = 'ready'
            if test:
                self.env.cr.rollback()
            if run_import:
                self.run_import_process(test=test)
        except Exception as e:
            self.env.cr.rollback()
            self.state = 'error'
            self.status_message = repr(e)
            self.env.cr.commit()
            raise e



