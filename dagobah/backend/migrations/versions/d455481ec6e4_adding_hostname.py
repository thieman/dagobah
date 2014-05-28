"""adding notes column

Revision ID: d455481ec6e4
Revises: 4de8f69a75d1
Create Date: 2014-05-19 17:00:00.00023

"""

# revision identifiers, used by Alembic.
revision = 'd455481ec6e4'
down_revision = '4de8f69a75d1'

from alembic import op
import sqlalchemy as sa


def upgrade():
    conn = op.get_bind()
    columns = conn.execute('select * from dagobah_task limit 1')._metadata.keys
    if 'hostname' not in columns:
        op.add_column('dagobah_task', sa.Column('hostname', sa.String(1000)))


def downgrade():
    op.drop_column('dagobah_task', 'hostname')
