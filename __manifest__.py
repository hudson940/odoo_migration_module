# -*- coding: utf-8 -*-
#################################################################################
# Author      : Anderson Martinez (andersonvidal94@gmail.com)
# License     : AGPL-3
#
#################################################################################
{
    'name': 'Migration from other odoo',
    "version": "1.0.0",
    "author": "Anderson Martinez",
    "maintainer": "Anderson Martinez",
    #'depends': ['queue_job'],
    'external_dependencies': {'python': ['odoorpc']},
    'data': [
        'data/migration.credentials.csv',
        # 'data/migration.model.csv',
        # 'data/migration.record.csv',
        'security/ir.model.access.csv',
        'views/migration_views.xml',
    ],
    "license": "AGPL-3",
}