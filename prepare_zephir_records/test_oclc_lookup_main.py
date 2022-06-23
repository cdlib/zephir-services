# TEST cmd line options
def test_main(setup):

    runner = CliRunner()
    result = runner.invoke(main)
    assert result.exit_code == 1
    assert 'Usage' in result.output

    result = runner.invoke(main, ['-t'])
    #assert result.exit_code == 0 
    assert 'Running tests ...' in result.output

    result = runner.invoke(main, ['--test'])
    #assert result.exit_code == 0
    assert 'Running tests ...' in result.output

    result = runner.invoke(main, ['1'])
    assert result.output ==  '{(1, 6567842, 9987701, 53095235, 433981287)}\n'

    result = runner.invoke(main, ['2'])
    assert result.output == '{(2, 9772597, 35597370, 60494959, 813305061, 823937796, 1087342349)}\n'

    result = runner.invoke(main, ['1', '2'])
    assert result.output == '{(2, 9772597, 35597370, 60494959, 813305061, 823937796, 1087342349), (1, 6567842, 9987701, 53095235, 433981287)}\n'

    # '123' is not in the test db
    result = runner.invoke(main, ['123'])
    assert result.output == 'set()\n'

