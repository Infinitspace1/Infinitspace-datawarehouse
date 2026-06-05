import unittest
from datetime import date

from shared.integrations.xero_nexudus_overdue import XeroNexudusOverdueLinker
from shared.nexudus.transformers.coworker_invoices import transform_coworker_invoice
from shared.nexudus.transformers.coworkers import transform_coworker


class TestXeroNexudusInvoiceLinking(unittest.TestCase):
    def test_transform_coworker_invoice_maps_core_fields(self):
        raw = {
            "Id": 1426606657,
            "UniqueId": "39c74bec-0878-47f1-99ca-b34113904abb",
            "CoworkerId": 1419974207,
            "CoworkerFullName": "Philipp Lange",
            "CoworkerBillingEmail": None,
            "CoworkerCompanyName": "premote GmbH",
            "CoworkerTeamNames": "premote",
            "BusinessId": 1420976475,
            "BusinessName": "Berlin - Mitte - Chausseestrasse 29",
            "InvoiceNumber": "C29-INV-2025.07-0001",
            "PaymentReference": "C29-INV-2025.07-0001",
            "BillToName": "premote GmbH",
            "BillToAddress": "Muehlenstrasse 8a",
            "BillToCity": "Berlin",
            "BillToPostCode": "14167",
            "BillToState": "Berlin",
            "BillToCountryName": "Germany",
            "BillToTaxIDNumber": "DE361125880",
            "Description": "Service Retainer",
            "DueDate": "2025-08-08T10:28:38Z",
            "TotalAmount": 2676.0,
            "PaidAmount": 2676.0,
            "DueAmount": 0.0,
            "TaxAmount": 76.0,
            "Paid": True,
            "Sent": True,
            "XeroInvoiceTransfered": True,
            "XeroPaymentTransfered": True,
            "CreatedOn": "2025-07-30T10:28:38Z",
            "UpdatedOn": "2025-09-08T09:06:25Z",
        }

        row = transform_coworker_invoice(raw, bronze_id=77, sync_run_id="sync-1")
        self.assertEqual(row["source_id"], 1426606657)
        self.assertEqual(row["coworker_id"], 1419974207)
        self.assertEqual(row["location_source_id"], 1420976475)
        self.assertEqual(row["invoice_number"], "C29-INV-2025.07-0001")
        self.assertEqual(str(row["total_amount"]), "2676.00")
        self.assertTrue(row["xero_invoice_transferred"])

    def test_transform_coworker_invoice_normalizes_due_date_to_business_timezone(self):
        raw = {
            "Id": 1,
            "InvoiceNumber": "GB-INV-2026.05-0189",
            "DueDate": "2026-05-21T22:00:00Z",
        }

        row = transform_coworker_invoice(raw, bronze_id=1, sync_run_id="sync-1")

        self.assertEqual(row["due_date"].date(), date(2026, 5, 22))
        self.assertIsNone(row["due_date"].tzinfo)

    def test_transform_coworker_invoice_maps_processing_status(self):
        raw = {
            "Id": 1,
            "InvoiceNumber": "GB-INV-2026.05-0189",
            "Status": "Processing",
        }

        row = transform_coworker_invoice(raw, bronze_id=1, sync_run_id="sync-1")

        self.assertEqual(row["invoice_status"], "Processing")
        self.assertTrue(row["processing"])

    def test_transform_coworker_invoice_uses_history_awaiting_as_processing(self):
        raw = {
            "Id": 1,
            "InvoiceNumber": "GB-INV-2026.05-0189",
            "Paid": False,
            "Void": False,
            "CreditNote": False,
        }
        histories = [
            {
                "Name": "Payment Result",
                "Description": "AWAITING: Waiting for the money to clear from the customer's account.",
                "IsProblem": False,
                "CreatedOn": "2026-05-22T04:57:45Z",
            }
        ]

        row = transform_coworker_invoice(raw, bronze_id=1, sync_run_id="sync-1", histories=histories)

        self.assertEqual(row["invoice_status"], "Processing")
        self.assertTrue(row["processing"])
        self.assertEqual(row["payment_failure_count"], 0)

    def test_transform_coworker_invoice_failed_payment_is_not_processing(self):
        raw = {
            "Id": 1,
            "InvoiceNumber": "GB-INV-2026.05-0189",
            "Paid": False,
            "Void": False,
            "CreditNote": False,
        }

        histories = [
            {
                "Name": "Payment Result",
                "Description": "FAILED: Insufficient funds.",
                "IsProblem": True,
                "CreatedOn": "2026-05-21T04:57:45Z",
            }
        ]
        row = transform_coworker_invoice(raw, bronze_id=1, sync_run_id="sync-1", histories=histories)
        self.assertEqual(row["invoice_status"], "Payment Failed")
        self.assertFalse(row["processing"])
        self.assertEqual(row["payment_failure_count"], 1)

    def test_transform_coworker_invoice_mandate_revoked_is_not_processing(self):
        raw = {
            "Id": 1,
            "InvoiceNumber": "GB-INV-2026.05-0192",
            "Paid": False,
            "Void": False,
            "CreditNote": False,
        }
        histories = [
            {
                "Name": "Payment Result",
                "Description": "FAILED: Existing regular payment subscription was not valid and has been revoked.",
                "IsProblem": True,
                "CreatedOn": "2026-05-26T09:29:20Z",
            }
        ]
        row = transform_coworker_invoice(raw, bronze_id=1, sync_run_id="sync-1", histories=histories)
        self.assertEqual(row["invoice_status"], "Payment Failed")
        self.assertFalse(row["processing"])
        self.assertEqual(row["payment_failure_count"], 1)

    def test_transform_coworker_invoice_awaiting_after_failure_is_processing(self):
        raw = {"Id": 1, "InvoiceNumber": "GB-INV-X"}
        histories = [
            {
                "Name": "Payment Result",
                "Description": "FAILED: Insufficient funds.",
                "IsProblem": True,
                "CreatedOn": "2026-05-21T04:57:45Z",
            },
            {
                "Name": "Payment Result",
                "Description": "AWAITING: Waiting for the money to clear.",
                "IsProblem": False,
                "CreatedOn": "2026-05-23T04:57:45Z",
            },
        ]
        row = transform_coworker_invoice(raw, bronze_id=1, sync_run_id="sync-1", histories=histories)
        self.assertEqual(row["invoice_status"], "Processing")
        self.assertTrue(row["processing"])
        self.assertEqual(row["payment_failure_count"], 1)

    def test_transform_coworker_invoice_awaiting_exception_is_not_processing(self):
        # Nexudus prefixes no-mandate errors "AWAITING:" with IsProblem=false.
        # These can never clear, so they must surface as a failure (visible),
        # not be hidden as an in-flight collection.
        raw = {
            "Id": 1,
            "InvoiceNumber": "INV-2026.05-7322",
            "Paid": False,
            "Void": False,
            "CreditNote": False,
        }
        histories = [
            {
                "Name": "Payment Result",
                "Description": (
                    "AWAITING: Exception of type 'Nexudus.Coworking."
                    "CoworkerPaymentProcessing.Exceptions.NotPreAuthFoundException' "
                    "was thrown."
                ),
                "IsProblem": False,
                "CreatedOn": "2026-05-22T04:57:45Z",
            }
        ]
        row = transform_coworker_invoice(raw, bronze_id=1, sync_run_id="sync-1", histories=histories)
        self.assertEqual(row["invoice_status"], "Payment Failed")
        self.assertFalse(row["processing"])
        self.assertEqual(row["payment_failure_count"], 1)

    def test_transform_coworker_maps_email_and_billing_fields(self):
        raw = {
            "Id": 1419974207,
            "UniqueId": "48023cee-a8bb-4544-b26c-b458ba3bec9b",
            "CoworkerType": 2,
            "FullName": "Philipp Lange",
            "Email": "philipp@premote.de",
            "BillingEmail": None,
            "BillingName": "premote GmbH",
            "CompanyName": "premote GmbH",
            "Businesses": [1376491116, 1420976475],
            "InvoicingBusinessId": 1420976475,
            "InvoicingBusinessName": "Berlin - Mitte - Chausseestrasse 29",
            "TeamName": "premote",
            "TeamNames": "premote",
            "TeamIds": "1415743277",
            "CoworkerContractIds": "1417940585,1417940586",
            "CoworkerContractTariffNames": "Private Office,- Discounts",
            "BillingDay": 10,
            "TariffId": 1415274533,
            "TariffName": "Private Office @Chausseestrasse",
            "Active": True,
            "Archived": False,
            "UserActive": True,
            "NotifyOnNewInvoice": True,
            "NotifyOnNewPayment": True,
            "NotifyOnFailedPayment": True,
            "DoNotProcessInvoicesAutomatically": False,
            "RegistrationDate": "2025-07-30T10:16:32Z",
            "RenewalDate": "2026-04-09T22:00:00Z",
            "CreatedOn": "2025-07-30T10:16:32Z",
            "UpdatedOn": "2026-04-01T00:04:57Z",
        }

        row = transform_coworker(raw, bronze_id=88, sync_run_id="sync-2")
        self.assertEqual(row["source_id"], 1419974207)
        self.assertEqual(row["email"], "philipp@premote.de")
        self.assertEqual(row["billing_name"], "premote GmbH")
        self.assertEqual(row["business_ids"], "1376491116,1420976475")
        self.assertTrue(row["active"])

    def test_linker_matches_on_invoice_number_and_prefers_billing_email(self):
        linker = XeroNexudusOverdueLinker()
        xero_invoices = [
            {
                "invoice_number": "C29-INV-2025.07-0001",
                "reference": "C29-INV-2025.07-0001",
                "location_source_id": 1420976475,
            }
        ]
        nexudus_invoices = [
            {
                "source_id": 1426606657,
                "coworker_id": 1419974207,
                "coworker_name": "Philipp Lange",
                "coworker_billing_email": None,
                "location_source_id": 1420976475,
                "invoice_number": "C29-INV-2025.07-0001",
                "payment_reference": "C29-INV-2025.07-0001",
                "bill_to_name": "premote GmbH",
            }
        ]
        coworkers = [
            {
                "source_id": 1419974207,
                "full_name": "Philipp Lange",
                "email": "billing@premote.de",
                "billing_email": "finance@premote.de",
                "billing_name": "premote GmbH",
            }
        ]

        rows = linker.link_invoices(xero_invoices, nexudus_invoices, coworkers)
        self.assertTrue(rows[0]["link_matched"])
        self.assertEqual(rows[0]["link_match_reason"], "invoice_number")
        self.assertEqual(rows[0]["recipient_email"], "finance@premote.de")
        self.assertEqual(rows[0]["nexudus_coworker_id"], 1419974207)

    def test_linker_falls_back_to_xero_reference(self):
        linker = XeroNexudusOverdueLinker()
        rows = linker.link_invoices(
            [
                {
                    "invoice_number": "XERO-LOCAL-123",
                    "reference": "C29-INV-2025.07-0001",
                    "location_source_id": 1420976475,
                }
            ],
            [
                {
                    "source_id": 1426606657,
                    "coworker_id": 1419974207,
                    "coworker_name": "Philipp Lange",
                    "coworker_billing_email": "finance@premote.de",
                    "location_source_id": 1420976475,
                    "invoice_number": "C29-INV-2025.07-0001",
                    "payment_reference": "C29-INV-2025.07-0001",
                    "bill_to_name": "premote GmbH",
                }
            ],
            [],
        )
        self.assertTrue(rows[0]["link_matched"])
        self.assertEqual(rows[0]["link_match_reason"], "xero_reference")
        self.assertEqual(rows[0]["recipient_email"], "finance@premote.de")

    def test_linker_returns_unmatched_when_no_nexudus_invoice_exists(self):
        linker = XeroNexudusOverdueLinker()
        rows = linker.link_invoices(
            [{"invoice_number": "INV-404", "reference": None, "location_source_id": 5}],
            [],
            [],
        )
        self.assertFalse(rows[0]["link_matched"])
        self.assertEqual(rows[0]["link_match_reason"], "unmatched")


if __name__ == "__main__":
    unittest.main()
