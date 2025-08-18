"""new table: tg_accounts, channels, groups, reposts

Revision ID: 445360cd5212
Revises:
Create Date: 2024-08-01 14:21:21.786520

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '445360cd5212'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'channels',
        sa.Column('guid', sa.Uuid(), nullable=False),
        sa.Column('url', sa.String(), nullable=False),
        sa.Column('telegram_channel_id', sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint('guid')
    )
    op.create_table(
        'groups',
        sa.Column('guid', sa.Uuid(), nullable=False),
        sa.Column('channel_guid', sa.Uuid(), nullable=False),
        sa.Column('url', sa.String(), nullable=False),
        sa.PrimaryKeyConstraint('guid')
    )
    op.create_table(
        'reposts',
        sa.Column('guid', sa.Uuid(), nullable=False),
        sa.Column('channel_guid', sa.Uuid(), nullable=False),
        sa.Column('repost_message_id', sa.Integer(), nullable=False),
        sa.Column('created_at', sa.Date(), nullable=False),
        sa.PrimaryKeyConstraint('guid')
    )
    op.create_table(
        'tg_accounts',
        sa.Column('guid', sa.Uuid(), nullable=False),
        sa.Column('channel_guid', sa.Uuid(), nullable=True),
        sa.Column('telegram_id', sa.Integer(), nullable=False),
        sa.Column('last_datetime_pause', sa.DateTime(), nullable=True),
        sa.Column('pause_in_seconds', sa.Integer(), nullable=True),
        sa.Column('phone_number', sa.Integer(), nullable=False, unique=True),
        sa.Column('string_session', sa.String(), nullable=False),
        sa.Column('status', sa.String(), nullable=False),
        sa.PrimaryKeyConstraint('guid')
    )


def downgrade() -> None:
    op.drop_table('tg_accounts')
    op.drop_table('reposts')
    op.drop_table('groups')
    op.drop_table('channels')
