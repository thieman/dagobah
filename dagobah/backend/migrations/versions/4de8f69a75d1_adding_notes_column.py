"""adding notes column

Revision ID: 4de8f69a75d1
Revises: 2ab7af991b87
Create Date: 2013-10-29 15:43:13.132113

"""

# revision identifiers, used by Alembic.
revision = '4de8f69a75d1'
down_revision = '2ab7af991b87'

from alembic import op
import sqlalchemy as sa


def upgrade():
    conn = op.get_bind()
    columns = conn.execute('select * from dagobah_job limit 1')._metadata.keys
    if 'notes' not in columns:
        op.add_column('dagobah_job', sa.Column('notes', sa.String(1000)))


def downgrade():
    op.drop_column('dagobah_job', 'notes')
