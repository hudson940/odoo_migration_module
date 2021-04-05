from odoo import models, fields, api
from odoo.addons.queue_job.job import job
import odoorpc
import logging
import json
from odoo.exceptions import ValidationError
from odoo.tests import Form
import datetime
_log = logging.getLogger(__name__)

def get_chunks(iterable, n=1000):
    for i in range(0, len(iterable), int(n)):
        yield iterable[i:i + int(n)]

# creating an user raises: 'You cannot create a new user from here.
#  To create new user please go to configuration panel.'

BASE_MODEL_PREFIX = ['ir.', 'mail.', 'base.', 'bus.', 'report.', 'account.', 'res.users', 'stock.location', 'res.',
                     'product.pricelist', 'product.product', 'stock.picking.type','uom.','crm.team', 'stock.warehouse', 'stock.picking']
# todo: add bool field on migration.model like use_same_id
MODELS_WITH_EQUAL_IDS = ['res.partner', 'product.product', 'product.template', 'product.category', 'seller.instance', 'uom.uom', 'res.users']
WITH_AUTO_PROCESS = ['sale.order', 'purchase.order', 'update_product_template_costs', 'account.invoice', 'stock.picking']
COMPUTED_FIELDS_TO_READ = ['invoice_ids']

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


    def map_record(self):
        if self.new_id:
            return self.new_id
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
        if self.migration_model.match_records_by_name and (has_name or has_complete_name) and name:
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
        company_id = company_id or self.company_id.id
        if company_id:
            domain += ['|', ('company_id', '=', company_id), ('company_id', '=', False)]
        rec = self.search(domain)
        rec_new_id = rec.filtered(lambda r: r.new_id)
        if rec_new_id:
            return rec_new_id[0].new_id
        if len(rec) > 1:
            rec = rec[0]
        if rec.data and create:
            data = json.loads(rec.data)
            if company_id and data.get('company_id'):
                data['company_id'] = rec.company_id
            return rec.get_or_create_new_id(data, field_type=rec.type, relation=model, test=test, company_id=company_id)
        return 0

    def prepare_vals(self, data={}, fields_mapping={}, model='', test=False, company_id=0, migration_model=None):
        if not data and self.data:
            data = json.loads(self.data)
        if not fields_mapping and model:
            fields_mapping = self.env[model].fields_get()
        vals = {}
        migration_model = migration_model or self.migration_model
        in_status = migration_model.import_in_state
        company_id = company_id or self.company_id.id or migration_model.company_id.id
        omit_fields = migration_model.omit_fields.split(',') if migration_model.omit_fields else []
        for key in data:
            field_map = fields_mapping.get(key) or {}
            if not field_map or key in omit_fields:
                # Field does not exist in new odoo
                continue
            if key in ('id', 'display_name'):
                continue
            if key == 'company_id' and company_id:
                vals[key] = company_id
                continue
            if in_status and key == 'state':
                vals[key] = in_status
                continue
            value = data[key]
            if isinstance(value, (list, tuple)):
                field_type = field_map.get('type')
                if field_type == 'many2one':
                    # value is a tuple with (id, name)
                    try:
                        new_id = self.browse().get_or_create_new_id(value, field_map=field_map, test=test, company_id=company_id)
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
                        test=test, company_id=migration_model.company_id.id) for old in value]
                    values = [v for v in values if v]
                    if values:
                        vals[key] = [[6, 0, values]]

            else:
                # simple value, int, str
                vals[key] = value
        return vals

    def get_or_create_new_id(self, value=None, field_map=False,  field_type='', relation='', flag_try_old_id=False, test=False, company_id=0, force_create=False):
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
            # if field map we can try to create the record here
            id = self.get_new_id(relation, old_id, company_id=company_id, create=bool(field_map))
            if id:
                return id
        elif isinstance(value, dict):
            old_id = value.get('id') or self.old_id
            name = value.get('name')
            raw_vals = value
            id = self.get_new_id(relation, old_id, create=bool(field_map))
            if id:
                return id
        if self.migration_model.match_records_by_name and (has_name or has_complete_name) and name:
            domain = [('complete_name' if has_complete_name else 'name', '=', name)]
            has_company = hasattr(res_model, 'company_id')
            rec_no_company = False  # some records are shared between companies
            if has_company and company_id:
                rec_no_company = res_model.search(domain + [('company_id', '=', False)], limit=1).id
                domain.append(('company_id', '=', company_id))
            new_rec = res_model.search(domain, limit=1).id
            if not new_rec and rec_no_company:
                new_rec = rec_no_company
        if not new_rec and flag_try_old_id:
            new_rec = res_model.browse(old_id).exists().id
        if not new_rec:
            omit = self.migration_model.only_fetch_data or ((relation and [True for r in BASE_MODEL_PREFIX if relation.startswith(r)]))
            allowed = force_create
            if omit and not allowed:
                _log.warning('try to create a forbidden model record %s for %s' % (relation, [old_id, name]))
                return 0
            if raw_vals:
                vals = self.prepare_vals(raw_vals, model=relation, company_id=company_id)
                try:
                    new_rec = res_model.create(vals).id
                except Exception as e:
                    if test:
                        self.env.cr.rollback()
                    else:
                        self.env.cr.commit()
                    _log.exception(e)
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
                            vals = self.prepare_vals(data[0], model=relation, company_id=company_id, migration_model=migration_model)
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
    only_fetch_data = fields.Boolean()
    threads = fields.Integer(help='for parallel workers, if it\'s 0 will execute synchronous')
    migration_record_ids = fields.One2many('migration.record', 'migration_model')
    status_message = fields.Text(stored=True)
    import_in_state = fields.Char()
    read_one2many_fields = fields.Boolean()
    match_records_by_name = fields.Boolean(help="If true will match records by name or complete name", default=True)
    total_records = fields.Integer()
    extra_domain = fields.Char(help='domain extra in json format [["field", "operator", "value"]]', default='[["active","=",true]]')

    max_deep_level = fields.Integer(help="limit for recursion", default=1)
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
    has_auto_process = fields.Boolean(compute='_compute_progress')

    # temporal fields
    account_id = fields.Many2one('account.account')

    def compute_fields_mapping(self, dependencies=[]):
        for rec in self:
            try:
                omit_fields = []
                if self.omit_fields:
                    omit_fields = self.omit_fields.split(',')
                conn = self.conn()
                try:
                    res_model = self.env[rec.model]
                    if res_model._transient:
                        return
                except KeyError:
                    res_model = conn.env[rec.model]

                old_res_model = conn.env[rec.model]
                old_model_fields = old_res_model.fields_get()
                model_fields = res_model.fields_get() if not self.only_fetch_data else old_model_fields
                stored_fields = [f for f in model_fields if model_fields[f].get('store', True) or f in COMPUTED_FIELDS_TO_READ]
                old_fields_list = []
                fields_mapping = {}
                if not dependencies:
                    dependencies = rec.dependency_ids.search(['|', ('company_id', '=', rec.company_id.id), ('company_id','=', False)]).mapped('model')
                for field in stored_fields:
                    if field in omit_fields:
                        continue
                    new_field = model_fields[field]
                    if new_field.get('type') == 'one2many' and not rec.read_one2many_fields:
                        continue
                    old_field = old_model_fields.get(field)
                    if old_field or self.only_fetch_data:
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
            rec.has_auto_process = rec.model in WITH_AUTO_PROCESS
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

    def set_ready(self):
        for rec in self:
            rec.state = 'ready'

    def run_test(self, show_confirmation=True):
        try:
            self.prepare_records_from_old_server(test=True)
            self.env.cr.rollback()
        except Exception as e:
            raise ValidationError('Test Failed\n %s'% repr(e))
        if show_confirmation:
            raise ValidationError('Test Success')

    def button_fetch(self):
        self.prepare_records_from_old_server(run_import=False)

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
        if self.state != 'ready' or self.only_fetch_data:
            return  # raise ValidationError('Fetch data is no ready')
        if test:
            self.run_import_batch(self.migration_record_ids, test=test)
        if self.threads > 0:
            self.state = 'importing'
            n_batch = (self.total_records or len(self.migration_record_ids)) / self.threads
            chunks = get_chunks(self.migration_record_ids, n_batch)
            for batch in chunks:
                self.with_delay().run_import_batch(batch)

            # run in jobs
        else:
            for batch in get_chunks(self.migration_record_ids, 500):
                self.run_import_batch(batch)
            self.state = 'done'

    @job
    def run_import_batch(self, migration_record_ids, test=False):
        sql_errors = 0
        records = migration_record_ids.filtered(lambda r: not r.new_id)
        chunks = get_chunks(records, 100)
        for chunk in chunks:
            for rec in chunk:
                try:
                    rec.map_record()
                    if rec.new_id:
                        continue
                    new_obj = rec.get_or_create_new_id(test=test, force_create=True)
                except Exception as e:
                    _log.exception(e)
                    if test:
                        self.env.cr.rollback()
                    else:
                        try:
                            self.env.cr.commit() # to avoid InFailedSqlTransaction transaction block
                        except Exception as e2:
                            _log.exception(e2)
                            if sql_errors > 200:
                                raise e2
                            sql_errors += 1
                            self.env.cr.rollback()
                    rec.state = 'error'
                    rec.state_message = repr(e)
            if test:
                self.env.cr.rollback()
            else:
                self.env.cr.commit()

    def map_records(self):
        if not self.migration_record_ids:
            self.prepare_records_from_old_server(run_import=False)
        for batch in get_chunks(self.migration_record_ids):
            for rec in batch:
                rec.map_record()
            self.env.cr.commit()

    def auto_process(self):
        if self.model not in WITH_AUTO_PROCESS:
            raise ValidationError('Method Only available for model sale.order')
        if self.model == 'update_product_template_costs':
            # todo: temporal solution to update product costs
            if not self.migration_record_ids and self.fields_mapping:
                # products is a list of ['default_code','cost']
                products = json.loads(self.fields_mapping)
                self.total_records = len(products)
                self.migration_record_ids = [(0, 0, {'name': p[0], 'data': p[1]}) for p in products]

        if self.threads:
            n_batch = (self.total_records or len(self.migration_record_ids)) / self.threads
            chunks = get_chunks(self.migration_record_ids, n_batch)
            for batch in chunks:
                self.with_delay().run_auto_process(batch)
        else:
            self.run_auto_process(self.migration_record_ids)


    @job
    def run_auto_process(self, migration_record_ids):
        self.env.user.company_ids = self.env.user.company_id.search([])
        self.env.user.company_id = self.company_id
        if self.model == 'update_product_template_costs':
            assert self.account_id, 'account id is required'
            self.run_update_product_template_cost(migration_record_ids)
            return
        elif self.model in ('sale.order', 'purchase.order'):
            self.run_process_orders(migration_record_ids)
        elif self.model == 'stock.picking':
            self.run_process_picking(migration_record_ids)
        elif self.model == 'account.invoice':
            self.run_process_account_invoice(migration_record_ids)

    def run_process_account_invoice(self, migration_record_ids):
        # journal
        # account
        # tax

        account_move = self.env['account.move']
        partner_model = self.env['res.partner']
        res_user_model = self.env['res.users']
        mr_obj = self.env['migration.record']
        errors_journal = []
        errors_account = []
        errors_tax = []
        errors_payment_terms = []
        for rec in migration_record_ids:
            if rec.new_id and rec.state == 'done':
                continue

            try:
                old_data = json.loads(rec.data)
                payment_term_old_id = old_data.get('payment_term_id')
                invoice_line_ids = old_data.get('invoice_line_ids')
                journal_old_id = old_data.get('journal_id')

                if journal_old_id:
                    journal_id = mr_obj.get_new_id('account.journal', journal_old_id[0],company_id=self.company_id.id, create=False)
                    if not journal_id and journal_old_id[ 1 ] not in errors_journal:
                        errors_journal.append( journal_old_id[ 1 ] )

                if payment_term_old_id:
                    payment_term_id = mr_obj.get_new_id('account.payment.term', payment_term_old_id[0],company_id=self.company_id.id, create=False)
                    if not payment_term_id and payment_term_old_id[ 1 ] not in errors_payment_terms:
                        errors_payment_terms.append( payment_term_old_id[ 1 ] )

                invoice_line_ids = list(invoice_line_ids)
                inv_line_recs = rec.search([('model', '=', 'account.invoice.line'), ('old_id', 'in', invoice_line_ids)])
                for r in inv_line_recs:
                    if r.data:
                        line_data = json.loads( r.data )

                        account_old_id = line_data.get('account_id')
                        tax_ids = list(line_data.get('tax_line_ids')) if line_data.get('tax_line_ids', False) else False

                        if tax_ids:
                            line_taxes_recs = rec.search([('model', '=', 'account.tax'), ('old_id', 'in', tax_ids)])
                            for s in line_taxes_recs:
                                if s.data:
                                    tax_id = json.loads( s.data )
                                    tax_id = mr_obj.get_new_id('account.tax', tax_id.get('id'),company_id=self.company_id.id,create=False)

                        if account_old_id:
                            account_id = mr_obj.get_new_id('account.account', account_old_id[0], company_id=self.company_id.id,
                                                         create=False)

                            if not account_id and account_old_id[1] not in errors_account:
                                errors_account.append(account_old_id[1])


            except Exception as e:
                self.env.cr.rollback()
                rec.write({'state': 'error', 'state_message': repr(e)})
                _log.error(e)

        if errors_account or errors_payment_terms:
            raise ValidationError( '\n'.join(errors_account) + '\n\n\n' + '\n'.join(errors_payment_terms)  )

        for rec in migration_record_ids:
            if rec.new_id and rec.state == 'done':
                continue

            try:
                old_data = json.loads(rec.data)

                partner_id = old_data.get('partner_id')
                partner_shipping_id = old_data.get('partner_shipping_id')
                payment_term_id = old_data.get('payment_term_id')
                invoice_date = old_data.get('date_invoice')
                user_id = old_data.get('user_id')
                currency_id = old_data.get('currency_id')
                team_id = old_data.get('team_id')
                origin = old_data.get('origin')
                type = old_data.get('type')
                state = old_data.get('state')
                invoice_line_ids = old_data.get('invoice_line_ids')
                journal_id = old_data.get('journal_id')
                number = old_data.get('number')
                user_id = old_data.get('user_id')
                invoice_date_due = old_data.get('invoice_date_due')
                invoice_id = old_data.get('id')
                shipping_invoice = old_data.get('shipping_invoice', False)

                if invoice_date:
                    invoice_date_check = invoice_date.split(" ")[ 0 ]

                    if datetime.datetime.strptime(invoice_date_check, '%Y-%m-%d') >= datetime.datetime.strptime("2021-03-08",'%Y-%m-%d') and self.company_id.id == 58:
                        continue
                    if datetime.datetime.strptime(invoice_date_check, '%Y-%m-%d') >= datetime.datetime.strptime("2021-03-07",'%Y-%m-%d') and self.company_id.id == 60:
                        continue

                    if datetime.datetime.strptime(invoice_date_check, '%Y-%m-%d') >= datetime.datetime.strptime("2021-03-12",'%Y-%m-%d') and self.company_id.id == 1:
                        continue

                if origin and not 'refund' in type:
                    so_exits_origin = self.env['sale.order'].search_count([('name', '=', origin)])
                    po_exits_origin = self.env['purchase.order'].search_count([('name', '=', origin)])

                    if shipping_invoice:
                        so_exits_origin = False
                        po_exits_origin = False

                    if so_exits_origin or po_exits_origin:
                        continue


                if partner_id:
                    create_invoice_line = []

                    inv_line_recs = rec.search([('model', '=', 'account.invoice.line'), ('old_id', 'in', invoice_line_ids),('company_id', '=', self.company_id.id)])
                    for r in inv_line_recs:
                        if r.data:
                            line_data = json.loads( r.data )
                            product_id = line_data.get('product_id')
                            price_unit = line_data.get('price_unit')
                            quantity  = line_data.get('quantity')
                            account_id = line_data.get('account_id')
                            tax_ids = list(line_data.get('invoice_line_tax_ids')) if line_data.get('invoice_line_tax_ids', False) else False
                            name = line_data.get('name')

                            create_line_taxes = []
                            if tax_ids:
                                line_taxes_recs = rec.search([('model', '=', 'account.tax'), ('old_id', 'in', tax_ids),('company_id', '=', self.company_id.id)])
                                for s in line_taxes_recs:
                                    #tax_id = mr_obj.get_new_id('account.tax', tax_id.get('id'),company_id=self.company_id.id,create=False)
                                    create_line_taxes.append( s.new_id )

                            account_id = mr_obj.get_new_id('account.account', account_id[0], company_id=self.company_id.id,
                                                         create=False)

                            create_invoice_line.append((0, 0, {
                                'name': name,
                                'product_id': product_id[ 0 ] if product_id else False,
                                'price_unit': float(price_unit),
                                'quantity': quantity,
                                'account_id': account_id,
                                'tax_ids': [(6, False, create_line_taxes )] if create_line_taxes else False
                            }))


                    """journal_id = mr_obj.get_new_id('account.journal', journal_id[ 0 ], company_id=self.company_id.id, create=False)
                    if not journal_id:
                        raise ValidationError('The journal is requiere to create the invoice old id %s please map it' % ( journal_id[ 0 ] ))

                    payment_term_id = mr_obj.get_new_id('account.payment.term',
                                                                              payment_term_id[0],
                                                                              company_id=self.company_id.id,
                                                                              create=False)"""

                    partner_id = partner_id[0] if partner_id else False
                    partner_exist = partner_model.browse( partner_id )

                    if payment_term_id:
                        payment_term_id = mr_obj.get_new_id('account.payment.term',
                                                                                  payment_term_id[0],
                                                                                  company_id=self.company_id.id,
                                                                                  create=False)

                    if not partner_exist:
                        rec.write({'state_message': 'partner not found %s' % ( partner_id ), 'state' : 'error'})
                        continue

                    #if not create_invoice_line:
                    #    rec.write({'state_message': 'no invoice lines', 'state' : 'error'})
                    #    continue



                    if partner_exist:

                        account_move_id = account_move.sudo().create({
                            'partner_id' : partner_id,
                            'partner_shipping_id' : partner_shipping_id[ 0 ] if partner_shipping_id else False,
                            'invoice_payment_term_id' : payment_term_id if payment_term_id else False,
                            'invoice_date' : invoice_date,
                            'user_id' : user_id[ 0 ] if user_id else False,
                            'currency_id' : self.env['res.currency'].search([('name', '=', currency_id[ 1 ])]).id,
                            'team_id' : team_id[ 0 ] if team_id else False,
                            'invoice_origin' : origin,
                            'type' : type,
                            'invoice_line_ids' : create_invoice_line or False,
                            #'journal_id' : journal_id[ 0 ] if journal_id else False,
                            'company_id' : self.company_id.id,
                            'invoice_user_id' : user_id[ 0 ] if user_id else False,
                            'invoice_date_due' : invoice_date_due
                        })
                        if state == 'open' or state == 'paid' and create_invoice_line:
                            account_move_id.post()

                        if state == 'cancel':
                            account_move_id.button_cancel()

                        rec.write({'state' : 'done', 'new_id' : account_move_id.id})

                        self.env.cr.commit()


            except Exception as e:
                self.env.cr.rollback()
                rec.write({'state': 'error', 'state_message': repr(e)})
                _log.error(e)

    def run_process_orders(self, migration_record_ids):
        has_sp_op_migration = self.search_count([('model', '=', 'stock.pack.operation')])  # from odoo10
        sale_obj = self.env[self.model]
        picking_fields = ['origin', 'note', 'state', 'date_done', 'carrier_id','carrier_tracking_ref',]
        if has_sp_op_migration:
            picking_fields.append('pack_operation_ids')
        conn = self.conn()
        mr_obj = self.env['migration.record']
        move_obj = self.env['account.move']
        company_id = self.company_id
        for rec in migration_record_ids:
            try:
                if not rec.new_id:
                    _log.warning('omit order because no new id')
                    continue
                so = sale_obj.browse(rec.new_id)

                if so.state != 'draft':
                    _log.warning('order %s is not in draft' % rec.name)
                    continue
                old_data = json.loads(rec.data)
                if round(so.amount_total) != round(old_data.get('amount_total')):
                    rec.write({'state': 'error', 'state_message': 'Amount total discrepancy'})
                    continue
                old_state = old_data.get('state')
                old_date = old_data.get('date_order')
                if old_state not in ('sale', 'purchase', 'done'):
                    _log.warning('order was in state %s' % old_state)
                    continue
                # Validate the order
                if self.model == 'sale.order':
                    so.action_confirm()
                    so.date_order = old_date
                elif self.model == 'purchase.order':
                    so.button_confirm()
                    so.date_approve = old_date
                else:
                    raise NotImplementedError(self.model)
                # get old delivery data
                old_sp_ids = old_data.get('picking_ids')
                sp_date = old_date
                if not old_sp_ids:
                    rec.write({'state': 'error', 'state_message': 'order without picking_ids'})
                    continue
                if old_sp_ids:
                    sp_rec = rec.search([('model', '=', 'stock.picking'), ('old_id', 'in', old_sp_ids)])
                    sp_data = [json.loads(r.data) for r in sp_rec if r.data]
                    if not sp_rec:
                        # get from old server
                        sp_data = conn.env['stock.picking'].search_read([('id', 'in', old_sp_ids)], picking_fields)
                    if not sp_data:
                        rec.write({'state': 'error', 'state_message': 'no stock picking data found'})
                        continue
                    validated_pickings = [p for p in sp_data if p.get('state') == 'done']
                    if not validated_pickings:
                        rec.write({'state': 'pending', 'state_message': 'no validated picking found'})
                        continue
                    sp_lines = []
                    if has_sp_op_migration:
                        # for V10
                        # todo add support for stock.move V13
                        for sp_val in validated_pickings:
                            sp_lines += self.get_sp_lines_from_op_lines(sp_val, conn=conn, mr_obj=mr_obj)

                    if not sp_lines:
                        rec.write({'state_message': 'no picking lines found'})
                        continue
                    sp = so.picking_ids
                    # set unique operation lines
                    unique_lines = self.get_sp_unique_move_lines(sp_lines, mr_obj, company_id, sp)
                    # set moves
                    sp.move_line_ids_without_package = unique_lines
                    # validate delivery
                    sp_date = sp_data[0].get('date_done')
                    sp.date = sp_date
                    sp.action_done()
                    sp.date_done = sp_date
                    # write the sp_id to old sp_rec
                    sp_rec.update({'new_id': sp.id, 'state': 'done'})
                invoices = False
                # create and validate invoice
                if old_data.get('invoice_ids'):
                    if self.model == 'sale.order':
                        invoices = so._create_invoices()
                    elif self.model == 'purchase.order':
                        action = so.with_context(create_bill=True).action_view_invoice()
                        form = Form(move_obj.with_context(action['context']))
                        invoices = form.save()
                    if invoices:
                        if hasattr(invoices, 'l10n_mx_edi_origin'):
                            # avoid to generate fe invoice
                            invoices.update({'l10n_mx_edi_origin': 0})
                        invoices.update({'date': sp_date,
                                         'invoice_date': sp_date,
                                         'invoice_date_due': sp_date,
                                         })
                        invoices.post()

                #commit changes
                self.env.cr.commit()

            except Exception as e:
                self.env.cr.rollback()
                rec.write({'state': 'error', 'state_message': repr(e)})
                _log.error(e)

    def run_update_product_template_cost(self, migration_record_ids):
        tmpl_obj = self.env['product.template']
        for rec in migration_record_ids:
            try:
                if rec.state == 'done':
                    continue
                default_code = rec.name
                cost = float(rec.data.replace(',', '.'))
                product_templates = tmpl_obj.search([('default_code', '=', default_code)])
                for product_template in product_templates:
                    product_template.product_variant_ids._change_standard_price(cost, counterpart_account_id=self.account_id.id)
                self.env.cr.commit()
            except Exception as e:
                self.env.cr.rollback()
                rec.write({'state': 'error', 'state_message': repr(e)})

    @staticmethod
    def get_sp_lines_from_op_lines(sp_val, conn, mr_obj):
        op_lines = sp_val.get('pack_operation_ids')
        if op_lines:
            sp_lines = mr_obj.search([('model', '=', 'stock.pack.operation'), ('old_id', 'in', op_lines)])
            if sp_lines:
                sp_lines = [json.loads(r.data) for r in sp_lines if r.data]
            else:
                # get from connection
                sp_lines = conn.env['stock.pack.operation'].search_read(
                    [('id', 'in', op_lines)],
                    ['product_qty', 'location_id', 'state', 'qty_done', 'product_id', 'location_dest_id',
                     'product_uom_id'])
            return sp_lines
        return []

    @staticmethod
    def get_sp_unique_move_lines(sp_lines, mr_obj, company_id, sp=None):
        unique_line_ids = []
        unique_lines = []
        for l in sp_lines:
            if l.get('id') not in unique_line_ids:
                unique_line_ids.append(l.get('id'))
                old_product_id = l.get('product_id')
                product_id = mr_obj.get_or_create_new_id(value=old_product_id, relation='product.product')
                location_id = mr_obj.get_or_create_new_id(value=l.get('location_id'), relation='stock.location',
                                                          company_id=company_id.id)
                location_dest_id = mr_obj.get_or_create_new_id(value=l.get('location_dest_id'),
                                                               relation='stock.location', company_id=company_id.id)
                qty = l.get('qty_done')
                product_uom_id = mr_obj.get_or_create_new_id(value=l.get('product_uom_id'), relation='uom.uom')
                if not location_id:
                    _log.warning('location %s not found will use default' % l.get('location_id'))
                if not location_dest_id:
                    _log.warning('location dest %s not found will use default' % l.get('location_dest_id'))
                if not (product_id and qty, product_uom_id):
                    raise ValidationError('missing product, qty, location, product_uom_id')
                unique_lines.append((0, 0, {
                    'product_id': product_id,
                    'qty_done': qty,
                    'location_id': location_id or sp.location_id.id,
                    'product_uom_id': product_uom_id,
                    'location_dest_id': location_dest_id or sp.location_dest_id.id,
                }))
        return unique_lines


    def run_process_picking(self, migration_record_ids):
        has_sp_op_migration = self.search_count([('model', '=', 'stock.pack.operation')])  # from odoo10
        rec_obj = self.env[self.model]
        picking_fields = ['origin', 'note', 'state', 'date_done', 'carrier_id','carrier_tracking_ref',]
        if has_sp_op_migration:
            picking_fields.append('pack_operation_ids')
        conn = self.conn()
        mr_obj = self.env['migration.record']
        company_id = self.company_id
        for rec in migration_record_ids:
            try:
                sp = rec_obj.browse(rec.new_id).exists()
                if sp and sp.state != 'draft':
                    _log.warning('Picking not in draft')
                    continue
                sp_val = json.loads(rec.data)
                old_state = sp_val.get('state')
                sp_lines = []
                if has_sp_op_migration:
                    # for V10
                    sp_lines = self.get_sp_lines_from_op_lines(sp_val, conn, mr_obj)
                if not sp_lines:
                    rec.write({'state_message': 'no picking lines found'})
                    continue

                if not sp:
                    del sp_val['move_lines']
                    sp_id = rec.get_or_create_new_id(value=sp_val, company_id=company_id.id, force_create=True)
                    sp = rec_obj.browse(sp_id)
                if not sp:
                    _log.warning('Picking not found %s' % sp_val.get('name'))
                    continue
                unique_lines = self.get_sp_unique_move_lines(sp_lines, mr_obj, company_id, sp)
                if not sp.move_line_ids_without_package:
                    sp.move_line_ids_without_package = unique_lines

                if old_state in ('done',):
                    sp_date = sp.date
                    sp.action_done()
                    sp.date_done = sp_date

                #commit changes
                self.env.cr.commit()

            except Exception as e:
                self.env.cr.rollback()
                rec.write({'state': 'error', 'state_message': repr(e)})
                # sql_errors = 0
                # try:
                #     self.env.cr.commit()
                # except Exception as e2:
                #     if sql_errors < 200:
                #         sql_errors += 1
                #         _log.exception(e2)
                #         self.env.cr.rollback()
                #     else:
                #         raise e2
                _log.error(e)

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
            try:
                new_model = self.env[self.model]
            except KeyError:
                new_model = conn.env[self.model]
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
                domain.append(('create_date', '<', str(self.date_to)))
            if self.migration_record_ids:
                domain.append(('id', 'not in', self.migration_record_ids.mapped('old_id')))
            if self.extra_domain:
                extra_domain = json.loads(self.extra_domain)
                domain += extra_domain
            fields_to_read = json.loads(self.old_fields_list)
            fields_to_read.append('display_name')
            old_records = old_model.search(domain, limit=limit)
            self.total_records = self.total_records + len(old_records)
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

    def delete_incomplete_orders(self):
        if self.model not in ('sale.order','purchase.order'):
            raise ValidationError('model not allowed')
        res_obj = self.env[self.model]
        rec_obj = self.env['migration.record']

        def cancel_order(order):
            domain = [('new_id', 'in', order.order_line.ids), ('company_id', '=', self.company_id.id)]

            if self.model == 'sale.order':
                rec_lines = rec_obj.search(domain + [('model', '=', 'sale.order.line')])
                order.action_cancel()
            elif self.model == 'purchase.order':
                rec_lines = rec_obj.search(domain + [('model', '=', 'purchase.order.line')])
                order.button_cancel()
            else:
                return
            rec_lines.unlink()

        for rec in self.migration_record_ids:
            try:
                if not rec.new_id:
                    rec.unlink()
                res_id = res_obj.browse(rec.new_id)
                sp = res_id.picking_ids
                if not sp:
                    cancel_order(res_id)
                    res_id.unlink()
                    rec.unlink()
                    continue
                # if none of sp has been confirmed
                if not sp.filtered(lambda s: s.state == 'done'):
                    sp_rec = rec_obj.search([('new_id', 'in', sp.ids), ('model', '=', 'stock.picking'), ('company_id', '=', self.company_id.id)])
                    sp_rec.unlink()
                    sp.unlink()
                    cancel_order(res_id)
                    res_id.unlink()
                    rec.unlink()

                elif rec.state == 'error':
                    rec.update({
                        'state': 'done',
                        'state_message': False,
                    })
            except Exception as e:
                _log.error(e)
                rec.update({
                    'state': 'error',
                    'state_message': repr(e),
                })
