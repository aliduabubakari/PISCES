from pisces.cli import main

if __name__ == "__main__":
    import sys

    sys.argv.insert(1, "report-view")
    main()
