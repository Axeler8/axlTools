testthat::test_that("fetchDriveData() survives the apocalypse", {

  # --- Mock Setup ---
  httptest::with_mock_api({
    # Mock API response
    fake_drive <- list(
      files = data.table::data.table(
        name = "test.csv",
        id = "123",
        drive_resource = list(list(
          modifiedTime = "2024-01-01T00:00:00Z",
          createdTime = "2024-01-01T00:00:00Z",
          size = 1024,
          mimeType = "text/csv",
          owners = list(list(displayName = "Axel the Raccoon"))
        ))
      )
    )

    # --- Core Tests ---
    testthat::test_that("parameter validation works", {
      # Missing both params
      testthat::expect_error(fetchDriveData(), "Must provide either filePattern or id")

      # Invalid autoSelect
      testthat::expect_error(
        fetchDriveData(filePattern = "test", autoSelect = "random"),
        "should be one of"
      )
    })

    testthat::test_that("authentication fails gracefully", {
      # Bad key path
      testthat::expect_error(
        fetchDriveData(filePattern = "test", keyFile = "ghost.pem"),
        "Authentication failed"
      )
    })

    # Add more tests here...
  })
})
