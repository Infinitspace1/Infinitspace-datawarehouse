/* =====================================================================
   teamandy_00_schema.sql  —  create the `teamandy` operational schema.

   This is the OPERATIONAL CRM database for the TeamAndy apps (migrated off
   Firebase), living in the warehouse DB alongside bronze/silver/ava. It is a
   purpose-named schema like `ava` — NOT a medallion (bronze/silver) layer.

   NOTE on RCSI: the app's atomic counter updates (teamandy.lead_list_statistics,
   teamandy.contacts.reply_count) want READ_COMMITTED_SNAPSHOT ON at the DATABASE
   level. Azure SQL Database has RCSI ON by default. We deliberately do NOT run
   ALTER DATABASE here — it is a shared DB (bronze/silver/ava). Verify once with:
     SELECT is_read_committed_snapshot_on FROM sys.databases WHERE name = DB_NAME();
   and enable it separately if it is 0.
   ===================================================================== */
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'teamandy')
    EXEC(N'CREATE SCHEMA teamandy AUTHORIZATION dbo;');
GO
