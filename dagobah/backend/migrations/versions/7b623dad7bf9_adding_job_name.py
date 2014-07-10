"""Adding job_name column

Revision ID: 7b623dad7bf9
Revises: d455481ec6e4
Create Date: 2014-06-16 15:35:00.00023

"""

# revision identifiers, used by Alembic.
revision = '7b623dad7bf9'
down_revision = 'd455481ec6e4'

from alembic import op
import sqlalchemy as sa


def upgrade():
    conn = op.get_bind()
    columns = conn.execute('select * from dagobah_task limit 1')._metadata.keys
    if 'hostname' not in columns:
        op.add_column('dagobah_task', sa.Column('job_name', sa.String(1000)))


def downgrade():
    op.drop_column('dagobah_task', 'job_name')
