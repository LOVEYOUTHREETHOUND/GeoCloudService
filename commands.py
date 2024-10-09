def data_extraction_internal():
    from src.data_extraction_service import main
    main()


def data_extraction_external():
    # @LYF Deal with this function
    from src.data_extraction_service.external.main import main
    main()
    pass